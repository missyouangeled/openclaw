import { createHash } from "node:crypto";
import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import {
  resolveOpenClawCrablineChannelDriverSelection,
  type ServerRequestEvent,
  type StartedOpenClawCrablineAdapter,
} from "@openclaw/crabline";
import { isRecord } from "openclaw/plugin-sdk/string-coerce-runtime";
import { expect, it, vi } from "vitest";
import {
  runQaGatewayFixture,
  stopQaGatewayFixture,
} from "../../../test/helpers/qa-gateway-cleanup.js";
import {
  createQaGatewayChild,
  startQaMockOpenAiServer,
  type MockOpenAiRequestSnapshot,
} from "../api.js";
import { createQaBusState } from "../src/bus-state.js";
import { createQaCrablineTransportAdapter } from "../src/crabline-transport.js";

let startedAdapter: StartedOpenClawCrablineAdapter | undefined;
let retainedResources = false;
function capturedAdapter() {
  return startedAdapter;
}
vi.mock("@openclaw/crabline", async (importOriginal) => {
  const actual = await importOriginal<typeof import("@openclaw/crabline")>();
  return {
    ...actual,
    // Capture the public object returned to the unchanged transport. Arguments, observer,
    // native events and return value all pass through without replacement.
    startOpenClawCrablineAdapter: async (
      ...args: Parameters<typeof actual.startOpenClawCrablineAdapter>
    ) => {
      const adapter = await actual.startOpenClawCrablineAdapter(...args);
      startedAdapter = adapter;
      return adapter;
    },
  };
});

const repoRoot = path.resolve(import.meta.dirname, "../../..");
type NativeRecord = ServerRequestEvent & { accepted?: boolean };

function required(name: string): string {
  const value = process.env[name]?.trim();
  if (!value) {
    throw new Error(`${name} is required for candidate proof`);
  }
  return value;
}

function sha256(bytes: Buffer | string): string {
  return createHash("sha256").update(bytes).digest("hex");
}

async function candidateIdentity() {
  const root = await fs.realpath(required("CRABLINE_CANDIDATE_ROOT"));
  const rows: string[] = [];
  async function visit(directory: string): Promise<void> {
    for (const name of (await fs.readdir(directory)).toSorted()) {
      const file = path.join(directory, name);
      const stat = await fs.lstat(file);
      if (stat.isDirectory()) {
        await visit(file);
      } else if (stat.isFile()) {
        const bytes = await fs.readFile(file);
        rows.push(
          `${path.relative(root, file).split(path.sep).join("/")}\0${sha256(bytes)}\0${bytes.length}\n`,
        );
      } else {
        throw new Error("Candidate package contains a non-regular entry");
      }
    }
  }
  await visit(root);
  const pkg = JSON.parse(await fs.readFile(path.join(root, "package.json"), "utf8")) as {
    name: string;
    exports: { ".": { import: string; types: string } };
  };
  expect(pkg.name).toBe("@openclaw/crabline");
  const exports: Record<string, string> = {};
  for (const [kind, relative] of Object.entries(pkg.exports["."])) {
    const file = await fs.realpath(path.resolve(root, relative));
    expect(file.startsWith(`${root}${path.sep}`)).toBe(true);
    exports[kind] = sha256(await fs.readFile(file));
  }
  const identity = {
    archive: sha256(await fs.readFile(required("CRABLINE_CANDIDATE_ARCHIVE"))),
    content: sha256(rows.toSorted().join("")),
    files: rows.length,
    exports,
  };
  expect(identity.archive).toBe(required("CRABLINE_CANDIDATE_ARCHIVE_SHA256"));
  expect(identity.content).toBe(required("CRABLINE_CANDIDATE_PACKAGE_SHA256"));
  return identity;
}

async function json<T>(url: string, signal: AbortSignal): Promise<T> {
  const response = await fetch(url, { signal });
  expect(response.ok).toBe(true);
  return (await response.json()) as T;
}

async function slackRuntimeIdentity() {
  const root = path.join(await fs.realpath(repoRoot), "dist/extensions/slack");
  const packageBytes = await fs.readFile(path.join(root, "package.json"));
  const pkg = JSON.parse(packageBytes.toString("utf8")) as {
    name: string;
    openclaw: { extensions: string[] };
  };
  expect(pkg.name).toBe("@openclaw/slack");
  expect(pkg.openclaw.extensions).toEqual([expect.stringMatching(/^\.\/index\.(?:c|m)?js$/u)]);
  const entry = await fs.realpath(path.resolve(root, pkg.openclaw.extensions[0]!));
  expect(path.dirname(entry)).toBe(root);
  return {
    entry,
    packageSha256: sha256(packageBytes),
    entrySha256: sha256(await fs.readFile(entry)),
    sourceSha256: sha256(await fs.readFile(path.join(repoRoot, "extensions/slack/index.ts"))),
  };
}

// These are separate test cells: a nonstreaming callback pass never qualifies defaults.
it.for(["nonstreaming", "default-streaming diagnostic"] as const)(
  "runs a public Slack callback roundtrip through the real Gateway: %s",
  { timeout: 180_000 },
  async (mode, { signal, onTestFinished }) => {
    if (retainedResources) {
      throw new Error("Prior candidate resources remain owned; do not start another fixture");
    }
    const identity = await candidateIdentity();
    const directory = await fs.mkdtemp(path.join(os.tmpdir(), "slack-crabline-candidate-"));
    const gatewayOwner = createQaGatewayChild();
    retainedResources = true;
    const fixture = new AbortController();
    const assertActive = () => {
      signal.throwIfAborted();
      fixture.signal.throwIfAborted();
    };
    const bus = createQaBusState();
    let transport: Awaited<ReturnType<typeof createQaCrablineTransportAdapter>> | undefined;
    let mock: Awaited<ReturnType<typeof startQaMockOpenAiServer>> | undefined;
    let gateway: Awaited<ReturnType<typeof gatewayOwner.start>> | undefined;
    let stopped = false;
    let transportClosed = false;
    let providerClosed = false;
    let completed = false;
    let effectiveConfig: unknown;
    let runtimeIdentity: Awaited<ReturnType<typeof slackRuntimeIdentity>> | undefined;
    const cells: Array<Record<string, unknown>> = [];
    const records = async (): Promise<NativeRecord[]> => {
      const file = path.join(directory, "artifacts/crabline/slack-provider-server.jsonl");
      const contents = await fs.readFile(file, "utf8");
      return contents
        .split("\n")
        .filter(Boolean)
        .map((line) => JSON.parse(line) as NativeRecord);
    };
    const outbound = () =>
      bus.getSnapshot().messages.filter((message) => message.direction === "outbound");
    const runBody = async () => {
      assertActive();
      startedAdapter = undefined;
      transport = await createQaCrablineTransportAdapter({
        outputDir: directory,
        selection: resolveOpenClawCrablineChannelDriverSelection({ channel: "slack" }),
        state: bus,
      });
      assertActive();
      const adapter = capturedAdapter();
      if (!adapter || !adapter.bindGateway || adapter.manifest.provider !== "slack") {
        throw new Error("Installed public Slack adapter is missing its callback contract");
      }
      const manifest = adapter.manifest;
      const probe = await adapter.probe();
      if (!isRecord(probe) || typeof probe.user_id !== "string") {
        throw new Error("Slack auth.test did not return its native bot identity");
      }
      const botUserId = probe.user_id;
      const nativeMessages = async (channel: string, threadId?: string) => {
        const method = threadId ? "conversations.replies" : "conversations.history";
        const response = await fetch(`${manifest.endpoints.apiRoot}${method}`, {
          method: "POST",
          headers: {
            "content-type": "application/json",
            authorization: `Bearer ${manifest.botToken}`,
          },
          body: JSON.stringify({ channel, ...(threadId ? { ts: threadId } : {}) }),
          signal,
        });
        expect(response.ok).toBe(true);
        const history = (await response.json()) as {
          ok: boolean;
          messages: Array<{ ts: string; text: string; user: string; thread_ts?: string }>;
        };
        expect(history.ok).toBe(true);
        return history.messages;
      };
      assertActive();
      mock = await startQaMockOpenAiServer();
      assertActive();
      const channelConfig = transport.createGatewayConfig({ baseUrl: mock.baseUrl });
      runtimeIdentity = await slackRuntimeIdentity();
      assertActive();
      gateway = await gatewayOwner.start({
        repoRoot,
        providerBaseUrl: `${mock.baseUrl}/v1`,
        transportBaseUrl: mock.baseUrl,
        providerMode: "mock-openai",
        primaryModel: "mock-openai/gpt-5.6-luna",
        alternateModel: "mock-openai/gpt-5.6-luna",
        forcedRuntime: "openclaw",
        controlUiEnabled: false,
        enabledPluginIds: [...transport.requiredPluginIds],
        runtimeEnvPatch: transport.createRuntimeEnvPatch(),
        mutateConfig: (cfg) => ({
          ...cfg,
          logging: { ...cfg.logging, level: "debug", consoleLevel: "debug" },
          agents: { ...cfg.agents, defaults: { ...cfg.agents?.defaults, typingMode: "never" } },
          messages: { ...cfg.messages, ...channelConfig.messages },
          channels: {
            ...cfg.channels,
            ...channelConfig.channels,
            slack: {
              ...channelConfig.channels?.slack,
              // Typing is separate; leave streaming untouched in its diagnostic.
              typingReaction: "",
              replyToMode: "off",
              accounts: {
                default: {
                  webhookPath: "/candidate/slack/events",
                  ...(mode === "nonstreaming"
                    ? { streaming: { mode: "off" as const, nativeTransport: false } }
                    : {}),
                },
              },
            },
          },
        }),
      });
      assertActive();
      await transport.waitReady({ gateway });
      assertActive();
      const loaded = [...gateway.logs().matchAll(/\[plugins\] loading slack from ([^\r\n]+)/gu)]
        .at(-1)?.[1]
        ?.trim();
      if (!loaded) {
        throw new Error("Gateway did not report its actual Slack executable entry");
      }
      expect(await fs.realpath(loaded)).toBe(runtimeIdentity.entry);
      const slack = gateway.cfg.channels?.slack;
      const accounts = slack?.accounts ?? {};
      const key = Object.hasOwn(accounts, "default")
        ? "default"
        : Object.keys(accounts).find((entry) => entry.trim().toLowerCase() === "default");
      const account = key === undefined ? undefined : accounts[key];
      effectiveConfig = {
        accountKey: key,
        webhookPath: account?.webhookPath ?? slack?.webhookPath,
        rootStreaming: slack?.streaming ?? null,
        accountStreaming: account?.streaming ?? null,
        // Report authored values; do not invent a runtime call from an inferred default.
        streaming: { ...slack?.streaming, ...account?.streaming },
      };

      const firstCursor = await json<{ cursor: number }>(
        `${mock.baseUrl}/debug/request-cursor`,
        signal,
      );
      const firstRecorderCursor = (await records()).length;
      const firstBusCursor = outbound().length;
      const unboundMarker = "p03-unbound-no-replay";
      assertActive();
      const unbound = await transport.sendInbound({
        conversation: { id: "D0000000001", kind: "direct" },
        senderId: "U0000000001",
        text: unboundMarker,
      });
      expect(unbound.id).toMatch(/^\d+\.\d+$/u);
      expect(await nativeMessages("D0000000001")).toEqual([
        expect.objectContaining({ ts: unbound.id, text: unboundMarker, user: "U0000000001" }),
      ]);
      await transport.waitForNoOutbound({ sinceIndex: firstBusCursor });
      expect(
        await json<MockOpenAiRequestSnapshot[]>(
          `${mock.baseUrl}/debug/requests?after=${firstCursor.cursor}`,
          signal,
        ),
      ).toEqual([]);
      cells.push({
        kind: "unbound",
        modelCursor: firstCursor.cursor,
        recorderCursor: firstRecorderCursor,
        busCursor: firstBusCursor,
        inboundId: unbound.id,
        nativeCommitted: true,
      });

      // This is P03's direct public binding. Automatic Gateway lifecycle wiring belongs to P04.
      assertActive();
      await adapter.bindGateway({
        baseUrl: gateway.baseUrl,
        cfg: gateway.cfg,
        signal: fixture.signal,
      });
      let parentId: string | undefined;
      for (const kind of mode === "nonstreaming"
        ? (["dm", "thread"] as const)
        : (["dm"] as const)) {
        const marker = `p03-${mode}-${kind}`;
        const reply = `callback-${kind}-reply`;
        const modelCursor = await json<{ cursor: number }>(
          `${mock.baseUrl}/debug/request-cursor`,
          signal,
        );
        const recorderCursor = (await records()).length;
        const busCursor = outbound().length;
        assertActive();
        const inbound = await transport.sendInbound({
          conversation: { id: "D0000000002", kind: "direct" },
          senderId: "U0000000001",
          text: `${marker}: reply exactly \`${reply}\``,
          ...(kind === "thread" ? { threadId: parentId } : {}),
        });
        expect(inbound.id).toMatch(/^\d+\.\d+$/u);
        const normalized = await transport.waitForOutbound({
          conversation: { id: "D0000000002", kind: "direct" },
          sinceIndex: busCursor,
          textIncludes: reply,
          ...(kind === "thread" ? { threadId: parentId } : {}),
          timeoutMs: 45_000,
        });
        expect(normalized.text).toBe(reply);
        expect(normalized.id).not.toBe("");
        expect(normalized.accountId).toBe("default");
        const requests = await json<MockOpenAiRequestSnapshot[]>(
          `${mock.baseUrl}/debug/requests?after=${modelCursor.cursor}`,
          signal,
        );
        const ownRequests = requests.filter((entry) => entry.allInputText.includes(marker));
        expect(ownRequests).toHaveLength(1);
        expect(ownRequests[0]!.outcome).toBe("success");
        expect(requests.every((entry) => !entry.allInputText.includes(unboundMarker))).toBe(true);
        const posts = (await records())
          .slice(recorderCursor)
          .filter(
            (event) =>
              event.method === "POST" &&
              event.path.endsWith("/api/chat.postMessage") &&
              event.accepted === true &&
              isRecord(event.body) &&
              event.body.text === reply,
          );
        expect(posts).toHaveLength(1);
        expect(posts[0]!.body).toMatchObject({
          channel: "D0000000002",
          ...(kind === "thread" ? { thread_ts: parentId } : {}),
        });
        const history = await nativeMessages(
          "D0000000002",
          kind === "thread" ? parentId : undefined,
        );
        const nativeReply = history.find((message) => message.text === reply);
        expect(nativeReply?.ts).toMatch(/^\d+\.\d+$/u);
        expect(nativeReply?.user).toBe(botUserId);
        expect(nativeReply?.thread_ts).toBe(kind === "thread" ? parentId : undefined);
        expect(normalized.threadId).toBe(kind === "thread" ? parentId : undefined);
        cells.push({
          kind,
          modelCursor: modelCursor.cursor,
          recorderCursor,
          busCursor,
          inboundId: inbound.id,
          nativeReplyId: nativeReply?.ts,
          normalizedId: normalized.id,
          threadId: normalized.threadId ?? null,
        });
        parentId ??= inbound.id;
      }
      const allRequests = await json<MockOpenAiRequestSnapshot[]>(
        `${mock.baseUrl}/debug/requests?after=${firstCursor.cursor}`,
        signal,
      );
      expect(allRequests.every((entry) => !entry.allInputText.includes(unboundMarker))).toBe(true);
      completed = true;
    };
    let body: Promise<void> | undefined;
    let bodyFailure: unknown;
    let cleanupPromise: Promise<void> | undefined;
    const cleanup = () =>
      (cleanupPromise ??= runQaGatewayFixture(
        async () => {
          expect(await candidateIdentity()).toEqual(identity);
          if (runtimeIdentity) {
            expect(await slackRuntimeIdentity()).toEqual(runtimeIdentity);
          }
          // Diagnostic failures cannot convert a failed cell into a pass. Gateway logs use
          // the owner's redaction contract; retain actual default-mode API failures.
          console.log(
            JSON.stringify({
              proof: "slack-public-callback-candidate",
              mode,
              completed,
              identity,
              effectiveConfig,
              cells,
              artifactDirectory: directory,
              runtime: { platform: process.platform, arch: process.arch, node: process.version },
              runtimeIdentity: runtimeIdentity
                ? {
                    packageSha256: runtimeIdentity.packageSha256,
                    entrySha256: runtimeIdentity.entrySha256,
                    sourceSha256: runtimeIdentity.sourceSha256,
                  }
                : null,
              calls: transport
                ? (await records()).map((event) => ({
                    method: event.method,
                    path: event.path,
                    accepted: event.accepted ?? null,
                  }))
                : [],
              model: mock
                ? (
                    await json<MockOpenAiRequestSnapshot[]>(
                      `${mock.baseUrl}/debug/requests`,
                      AbortSignal.timeout(5_000),
                    )
                  ).map((entry) => ({
                    cursor: entry.cursor,
                    outcome: entry.outcome,
                    errorCode: entry.errorCode ?? null,
                  }))
                : [],
              gatewayLogs: gateway?.logs().slice(-12_000) ?? null,
              limits: [
                "mock-model",
                "mock-native-slack",
                "direct-binding-only",
                "typing-off",
                "no-live-account",
                "runtime-alias-is-not-declaration-proof",
              ],
            }),
          );
        },
        () => fixture.abort(),
        async () => {
          await stopQaGatewayFixture({
            stop: async () => {
              const result = await gatewayOwner.stop();
              stopped =
                result.process === "confirmed-stopped" || result.process === "never-spawned";
              if (!stopped && result.errors.length === 0) {
                throw new Error("Gateway process stop is unconfirmed; retain downstream resources");
              }
              return result;
            },
          });
        },
        async () => {
          if (stopped) {
            // Join startup after owner.stop: a late returned adapter/provider remains owned here.
            await body?.catch(() => undefined);
            if (transport) {
              await transport.cleanupAfterGatewayStop();
            } else {
              await capturedAdapter()?.close();
            }
            transportClosed = true;
          }
        },
        async () => {
          if (stopped && transportClosed) {
            await mock?.stop();
            providerClosed = true;
          }
        },
        () => {
          retainedResources = !(stopped && transportClosed && providerClosed);
          // Raw recorder bytes stay available even when a later cleanup phase fails.
          console.log(
            JSON.stringify({
              artifactDirectory: directory,
              stopped,
              transportClosed,
              providerClosed,
              bodyFailure:
                bodyFailure instanceof Error
                  ? {
                      name: bodyFailure.name,
                      message: bodyFailure.message,
                    }
                  : (bodyFailure ?? null),
            }),
          );
        },
      ));
    onTestFinished(cleanup);
    const onAbort = () => {
      // The completion hook and the body both observe the same cleanup promise and its errors.
      void cleanup().catch(() => undefined);
    };
    signal.addEventListener("abort", onAbort, { once: true });
    try {
      if (signal.aborted) {
        await cleanup();
        signal.throwIfAborted();
      }
      await runQaGatewayFixture(() => {
        body = runBody().catch((error: unknown) => {
          bodyFailure = error;
          throw error;
        });
        return body;
      }, cleanup);
    } finally {
      signal.removeEventListener("abort", onAbort);
    }
  },
);

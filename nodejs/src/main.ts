import { decode, encode } from "@msgpack/msgpack";
import { Validator } from "@wzlin/valid";
import assertInstanceOf from "@xtjs/lib/js/assertInstanceOf";
import assertState from "@xtjs/lib/js/assertState";
import asyncRead from "@xtjs/lib/js/asyncRead";
import { spawn } from "node:child_process";
import { Duplex } from "node:stream";

export const spawnPyIpc = async ({
  python = "python",
  script,
  rootDir,
  stdin,
  environment: env = {},
}: {
  python?: string;
  script: string;
  rootDir: string; // Important for imports.
  stdin?: ArrayBuffer | Uint8Array | string;
  environment?: Record<string, string>;
}) => {
  let workerExpectedExit = false;
  const worker = spawn(python, [script], {
    stdio: ["pipe", "inherit", "inherit", "pipe", "pipe"],
    env: {
      ...process.env,
      // By default, Python does not support relative imports from the entry script, and does not have the script's containing or working directory in sys.path.
      PYTHONPATH: [
        rootDir,
        ...(process.env["PYTHONPATH"]?.split(":") ?? []),
      ].join(":"),
      ...env,
    },
  })
    .on("exit", (status, signal) => {
      if (!workerExpectedExit) {
        throw new Error(
          `Python exited with status ${status ?? "null"} and signal ${
            signal ?? "null"
          }`,
        );
      }
    })
    .on("error", (err) => {
      throw err;
    });
  const ipcSend = assertInstanceOf(worker.stdio[3], Duplex)
    .on("error", (err) => {
      throw err;
    })
    .on("end", () => {
      if (!workerExpectedExit) {
        throw new Error("IPC send pipe ended");
      }
    })
    .on("close", () => {
      if (!workerExpectedExit) {
        throw new Error("IPC send pipe closed");
      }
    });
  const ipcRecv = assertInstanceOf(worker.stdio[4], Duplex)
    .on("error", (err) => {
      throw err;
    })
    .on("end", () => {
      if (!workerExpectedExit) {
        throw new Error("IPC receive pipe ended");
      }
    })
    .on("close", () => {
      if (!workerExpectedExit) {
        throw new Error("IPC receive pipe closed");
      }
    });
  worker.stdin!.end(stdin);

  // Wait for worker to be ready, as otherwise our IPC data may be lost.
  assertState((await asyncRead(ipcRecv, 1))[0] === 0xfd);

  return {
    ipcSend,
    ipcRecv,
    expectExit: () => {
      workerExpectedExit = true;
    },
  };
};

export type PyIpc = Awaited<ReturnType<typeof spawnPyIpc>>;

export const createPyIpcQueue = (worker: PyIpc) => {
  let queueLoopStarted = false;
  const queue = Array<{
    request: Uint8Array;
    resolve: (v: any) => void;
    responseValidator: Validator<any>;
  }>();
  const maybeStartQueueLoop = async () => {
    if (queueLoopStarted) {
      return;
    }
    queueLoopStarted = true;
    while (queue.length) {
      const { request: reqRaw, resolve, responseValidator } = queue.shift()!;
      // IPC should be fast enough that we don't need to wait for callback/flush/drain and won't run out of memory.
      // We must prefix with the length; streaming with msgpack.Unpacker doesn't work:
      // - Even using open(..., buffer=0) or io.FileIO doesn't actually make `read()` nonblocking, so Python gets stuck forever.
      // - To make it actually nonblocking, we have to use `flags = fcntl.fcntl(3, fcntl.F_GETFL); fcntl.fcntl(3, fcntl.F_SETFL, flags | os.O_NONBLOCK)`.
      // - This then doesn't work with msgpack.Unpacker.
      const reqRawLen = Buffer.alloc(4);
      reqRawLen.writeUInt32LE(reqRaw.length);
      worker.ipcSend.write(reqRawLen);
      worker.ipcSend.write(reqRaw);
      // These `asyncRead` calls are safe because we have already attached "close" and "error" handlers on `ipcRecv`.
      const resRawLen = (await asyncRead(worker.ipcRecv, 4)).readUInt32LE();
      const resRaw = await asyncRead(worker.ipcRecv, resRawLen);
      resolve(responseValidator.parseRoot(decode(resRaw)));
    }
    queueLoopStarted = false;
  };
  return {
    request: <T, R>(
      typ: string,
      request: T,
      responseValidator: Validator<R>,
    ): Promise<R> =>
      new Promise<R>((resolve) => {
        queue.push({
          request: encode({
            ...request,
            $type: typ,
          }),
          resolve,
          responseValidator,
        });
        maybeStartQueueLoop();
      }),
  };
};

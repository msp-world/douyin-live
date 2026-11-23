/**
 * 抖音直播间弹幕/礼物桥接 MSP World（移植自 cortico-world-douyin-live）：
 * - 工具：douyin_live_status
 * - 信号：douyin.danmaku, douyin.gift, douyin.fansclub, douyin.room, douyin.audience
 */
import type { MSPNode } from "../shared/msp.ts";

export interface DouyinLiveSocketLike {
  send(data: string): void;
  close(): void;
  addEventListener(type: string, listener: (ev: any) => void): void;
}

export class DouyinLiveWorld {
  private readonly wsUrl: string;
  private readonly roomId: string;
  private readonly createSocket?: (url: string) => DouyinLiveSocketLike;
  private node: MSPNode | null = null;
  private ws: DouyinLiveSocketLike | null = null;
  private connected = false;
  private counts = { danmaku: 0, gift: 0, enter: 0, like: 0 };

  constructor(config: { wsUrl?: string; roomId: string }, createSocket?: (url: string) => DouyinLiveSocketLike) {
    this.wsUrl = config.wsUrl ?? "ws://127.0.0.1:8765";
    this.roomId = config.roomId;
    this.createSocket = createSocket;
  }

  attach(node: MSPNode): void {
    this.node = node;
    const sigs: [string, string][] = [
      ["douyin.danmaku", "抖音直播弹幕"],
      ["douyin.gift", "抖音直播礼物"],
      ["douyin.fansclub", "抖音粉丝团互动"],
      ["douyin.room", "抖音直播间状态变化"],
      ["douyin.audience", "抖音观众进场/点赞统计"],
    ];
    for (const [p, d] of sigs) node.declareSignal(p, d);

    node.tool(
      "douyin_live_status",
      "读取抖音直播弹幕桥连接状态与累计统计",
      () => ({ connected: this.connected, roomId: this.roomId, counts: { ...this.counts } }),
      { type: "object", additionalProperties: false, properties: {} },
    );
  }

  start(): void {
    if (this.ws) return;
    const sock = (this.createSocket ?? ((u) => new WebSocket(u) as unknown as DouyinLiveSocketLike))(this.wsUrl);
    this.ws = sock;
    sock.addEventListener("open", () => {
      this.connected = true;
      sock.send(JSON.stringify({ type: "subscribe", roomId: this.roomId }));
      this.node?.publish("douyin.room", { connected: true, roomId: this.roomId });
    });
    sock.addEventListener("message", (ev: any) => {
      try {
        const msg = JSON.parse(String(ev.data)) as Record<string, unknown>;
        this.handleBridgeMessage(msg);
      } catch {
        /* ignore */
      }
    });
    sock.addEventListener("close", () => {
      this.connected = false;
      this.ws = null;
      this.node?.publish("douyin.room", { connected: false, roomId: this.roomId });
    });
  }

  stop(): void {
    this.ws?.close();
    this.ws = null;
    this.connected = false;
  }

  handleBridgeMessage(msg: Record<string, unknown>): void {
    const kind = String(msg.type ?? msg.kind ?? "");
    if (kind === "danmaku" || kind === "chat") {
      this.counts.danmaku += 1;
      this.node?.publish("douyin.danmaku", { user: msg.user ?? "观众", text: msg.text ?? "" });
    } else if (kind === "gift") {
      this.counts.gift += 1;
      this.node?.publish("douyin.gift", { user: msg.user ?? "观众", gift: msg.gift ?? "礼物", count: msg.count ?? 1 });
    } else if (kind === "fansclub") {
      this.node?.publish("douyin.fansclub", msg);
    } else if (kind === "enter" || kind === "like") {
      if (kind === "enter") this.counts.enter += 1;
      else this.counts.like += 1;
      this.node?.publish("douyin.audience", { counts: { ...this.counts } });
    }
  }
}

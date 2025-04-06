
import type { Transport } from '@modelcontextprotocol/sdk/shared/transport.js';
import {
  JSONRPCMessage,
  JSONRPCMessageSchema,
} from '@modelcontextprotocol/sdk/types.js';
import type { Context } from 'hono';
import type { SSEStreamingApi } from 'hono/streaming';

const MAXIMUM_MESSAGE_SIZE = 4 * 1024 * 1024; // 4MB

export class SSETransport implements Transport {
  private _sessionId: string;

  onclose?: () => void;
  onerror?: (error: Error) => void;
  onmessage?: (message: JSONRPCMessage) => void;

  /**
   * Creates a new SSETransport, which will direct the MPC client to POST messages to messageUrl
   */
  constructor(
    private messageUrl: string,
    private stream: SSEStreamingApi,
  ) {
    this._sessionId = crypto.randomUUID();

    this.stream.onAbort(() => {
      void this.close();
    });
  }

  get sessionId(): string {
    return this._sessionId;
  }

  // start() is automatically called after MCP Server connects to the transport
  async start(): Promise<void> {
    if (this.stream == null) {
      throw new Error('Stream not initialized');
    }

    if (this.stream.closed) {
      throw new Error('SSE transport already closed!');
    }

    await this.stream.writeSSE({
      event: 'endpoint',
      data: `${this.messageUrl}?sessionId=${this.sessionId}`,
    });
  }

  async handlePostMessage(context: Context): Promise<Response> {
    if (this.stream?.closed == null) {
	return context.text('SSE connection not established', 500);
    }

    try {
      const contentType = context.req.header('content-type') || '';

      if (!contentType.includes('application/json')) {
        throw new Error(`Unsupported content-type: ${contentType}`);
      }

      // Check if the request body is too large
      const contentLength = Number.parseInt(
        context.req.header('content-length') || '0',
        10,
      );

      if (contentLength > MAXIMUM_MESSAGE_SIZE) {
        throw new Error(`Request body too large: ${contentLength} bytes`);
      }

      // Clone the request before reading the body to avoid stream issues
      const body = (await context.req.json()) as unknown;
      await this.handleMessage(body);

      return context.text('Accepted', 202);
    } catch (error) {
      this.onerror?.(error as Error);

      return context.text('Error', 400);
    }
  }

  /**
   * Handle a client message, regardless of how it arrived. This can be used to inform the server of messages that arrive via a means different than HTTP POST.
   */
  async handleMessage(message: unknown): Promise<void> {
    let parsedMessage: JSONRPCMessage;

    try {
      parsedMessage = JSONRPCMessageSchema.parse(message);
    } catch (error) {
      this.onerror?.(error as Error);
      throw error;
    }

    this.onmessage?.(parsedMessage);
  }

  async close(): Promise<void> {
    if (this.stream?.closed) {
      this.stream.abort();
    }
  }

  async send(message: JSONRPCMessage): Promise<void> {
    if (this.stream?.closed) {
      throw new Error('Not connected');
    }

    await this.stream.writeSSE({
      event: 'message',
      data: JSON.stringify(message),
    });
  }
}

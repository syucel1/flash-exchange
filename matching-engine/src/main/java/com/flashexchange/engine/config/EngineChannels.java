package com.flashexchange.engine.config;

import java.util.Objects;

/**
 * The three Aeron channel/stream pairs that connect the matching engine with its clients.
 * <p>
 * The same logical streams are addressed differently from the engine side and from the client side when running
 * over UDP, so factory methods exist for both:
 * <ul>
 *   <li><b>IPC</b> (same box): every channel is {@code aeron:ipc} on both sides and traffic goes through the
 *       shared-memory log buffers of a common media driver.</li>
 *   <li><b>UDP</b> (across boxes, reliable UDP): commands are unicast to the engine; execution events and market
 *       data use multi-destination-cast with dynamic control so any number of clients can subscribe and the engine
 *       sends once per message.</li>
 * </ul>
 *
 * @param commandChannel     channel of the command stream.
 * @param commandStreamId    command stream id (default 1001).
 * @param executionChannel   channel of the execution event stream.
 * @param executionStreamId  execution stream id (default 2001).
 * @param marketDataChannel  channel of the market data stream.
 * @param marketDataStreamId market data stream id (default 3001).
 */
public record EngineChannels(
    String commandChannel,
    int commandStreamId,
    String executionChannel,
    int executionStreamId,
    String marketDataChannel,
    int marketDataStreamId)
{
    /** Default command stream id. */
    public static final int COMMAND_STREAM_ID = 1001;

    /** Default execution event stream id. */
    public static final int EXECUTION_STREAM_ID = 2001;

    /** Default market data stream id. */
    public static final int MARKET_DATA_STREAM_ID = 3001;

    /** Default UDP port the engine receives commands on. */
    public static final int COMMAND_PORT = 40101;

    /** Default UDP control port of the execution stream. */
    public static final int EXECUTION_CONTROL_PORT = 40201;

    /** Default UDP control port of the market data stream. */
    public static final int MARKET_DATA_CONTROL_PORT = 40301;

    /** IPC channel. */
    public static final String IPC = "aeron:ipc";

    /**
     * Validate.
     */
    public EngineChannels
    {
        Objects.requireNonNull(commandChannel, "commandChannel");
        Objects.requireNonNull(executionChannel, "executionChannel");
        Objects.requireNonNull(marketDataChannel, "marketDataChannel");
    }

    /**
     * Shared-memory channels with the default stream ids (identical on both sides).
     *
     * @return IPC channels.
     */
    public static EngineChannels ipc()
    {
        return new EngineChannels(
            IPC, COMMAND_STREAM_ID, IPC, EXECUTION_STREAM_ID, IPC, MARKET_DATA_STREAM_ID);
    }

    /**
     * Engine-side UDP channels.
     *
     * @param engineHost             address the engine binds to and clients reach it on.
     * @param commandPort            port receiving commands.
     * @param executionControlPort   control port of the execution stream.
     * @param marketDataControlPort  control port of the market data stream.
     * @return channels for the engine.
     */
    public static EngineChannels udpEngineSide(
        final String engineHost,
        final int commandPort,
        final int executionControlPort,
        final int marketDataControlPort)
    {
        return new EngineChannels(
            "aeron:udp?endpoint=" + engineHost + ":" + commandPort, COMMAND_STREAM_ID,
            mdcPublication(engineHost, executionControlPort), EXECUTION_STREAM_ID,
            mdcPublication(engineHost, marketDataControlPort), MARKET_DATA_STREAM_ID);
    }

    /**
     * Client-side UDP channels matching {@link #udpEngineSide(String, int, int, int)}.
     *
     * @param engineHost            address of the engine.
     * @param clientHost            local address the client receives on (an ephemeral port is used).
     * @param commandPort           engine command port.
     * @param executionControlPort  engine execution control port.
     * @param marketDataControlPort engine market data control port.
     * @return channels for a client such as the trading service.
     */
    public static EngineChannels udpClientSide(
        final String engineHost,
        final String clientHost,
        final int commandPort,
        final int executionControlPort,
        final int marketDataControlPort)
    {
        return new EngineChannels(
            "aeron:udp?endpoint=" + engineHost + ":" + commandPort, COMMAND_STREAM_ID,
            mdcSubscription(clientHost, engineHost, executionControlPort), EXECUTION_STREAM_ID,
            mdcSubscription(clientHost, engineHost, marketDataControlPort), MARKET_DATA_STREAM_ID);
    }

    private static String mdcPublication(final String engineHost, final int controlPort)
    {
        return "aeron:udp?control=" + engineHost + ":" + controlPort + "|control-mode=dynamic";
    }

    private static String mdcSubscription(final String clientHost, final String engineHost, final int controlPort)
    {
        return "aeron:udp?endpoint=" + clientHost + ":0|control=" + engineHost + ":" + controlPort +
            "|control-mode=dynamic";
    }
}

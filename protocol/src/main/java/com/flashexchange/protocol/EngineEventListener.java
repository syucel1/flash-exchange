package com.flashexchange.protocol;

/**
 * Callback interface for messages published by the matching engine. Flyweights are only valid for the duration
 * of the callback. All methods default to no-op so consumers implement only what they need.
 */
public interface EngineEventListener
{
    /**
     * Private execution report (stream 2001).
     *
     * @param report decoded flyweight.
     */
    default void onExecutionReport(final ExecutionReportFlyweight report)
    {
    }

    /**
     * Trade between two orders (stream 2001).
     *
     * @param trade decoded flyweight.
     */
    default void onTradeExecution(final TradeExecutionFlyweight trade)
    {
    }

    /**
     * Incremental book update (stream 3001).
     *
     * @param update decoded flyweight.
     */
    default void onBookUpdate(final BookUpdateFlyweight update)
    {
    }

    /**
     * Book snapshot (stream 3001).
     *
     * @param snapshot decoded flyweight.
     */
    default void onBookSnapshot(final BookSnapshotFlyweight snapshot)
    {
    }

    /**
     * Public trade print (stream 3001).
     *
     * @param trade decoded flyweight.
     */
    default void onPublicTrade(final PublicTradeFlyweight trade)
    {
    }

    /**
     * A message that is not a valid engine output message (wrong schema, too short or unknown template).
     *
     * @param templateId template id from the header, or -1 if the header itself was unreadable.
     */
    default void onUnknownMessage(final int templateId)
    {
    }
}

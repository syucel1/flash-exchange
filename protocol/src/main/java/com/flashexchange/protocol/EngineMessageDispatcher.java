package com.flashexchange.protocol;

import org.agrona.DirectBuffer;

/**
 * Decodes engine output messages and dispatches them to an {@link EngineEventListener}. Reuses one flyweight per
 * message type; not thread safe.
 */
public final class EngineMessageDispatcher
{
    private final ExecutionReportFlyweight executionReport = new ExecutionReportFlyweight();
    private final TradeExecutionFlyweight tradeExecution = new TradeExecutionFlyweight();
    private final BookUpdateFlyweight bookUpdate = new BookUpdateFlyweight();
    private final BookSnapshotFlyweight bookSnapshot = new BookSnapshotFlyweight();
    private final PublicTradeFlyweight publicTrade = new PublicTradeFlyweight();

    /**
     * Decode one message.
     *
     * @param buffer   containing the message.
     * @param offset   of the message header.
     * @param length   of the message.
     * @param listener receiving the decoded message.
     * @return the template id dispatched, or -1 if the message was not recognised.
     */
    public int dispatch(
        final DirectBuffer buffer, final int offset, final int length, final EngineEventListener listener)
    {
        if (length < MessageHeader.LENGTH || MessageHeader.schemaId(buffer, offset) != MessageHeader.SCHEMA_ID)
        {
            listener.onUnknownMessage(-1);
            return -1;
        }

        final int templateId = MessageHeader.templateId(buffer, offset);
        switch (templateId)
        {
            case ExecutionReportFlyweight.TEMPLATE_ID:
                if (MessageHeader.isValid(buffer, offset, length, ExecutionReportFlyweight.BLOCK_LENGTH))
                {
                    listener.onExecutionReport(executionReport.wrapForDecode(buffer, offset));
                    return templateId;
                }
                break;

            case TradeExecutionFlyweight.TEMPLATE_ID:
                if (MessageHeader.isValid(buffer, offset, length, TradeExecutionFlyweight.BLOCK_LENGTH))
                {
                    listener.onTradeExecution(tradeExecution.wrapForDecode(buffer, offset));
                    return templateId;
                }
                break;

            case BookUpdateFlyweight.TEMPLATE_ID:
                if (MessageHeader.isValid(buffer, offset, length, BookLevelsFlyweight.BLOCK_LENGTH))
                {
                    listener.onBookUpdate(bookUpdate.wrapForDecode(buffer, offset));
                    return templateId;
                }
                break;

            case BookSnapshotFlyweight.TEMPLATE_ID:
                if (MessageHeader.isValid(buffer, offset, length, BookLevelsFlyweight.BLOCK_LENGTH))
                {
                    listener.onBookSnapshot(bookSnapshot.wrapForDecode(buffer, offset));
                    return templateId;
                }
                break;

            case PublicTradeFlyweight.TEMPLATE_ID:
                if (MessageHeader.isValid(buffer, offset, length, PublicTradeFlyweight.BLOCK_LENGTH))
                {
                    listener.onPublicTrade(publicTrade.wrapForDecode(buffer, offset));
                    return templateId;
                }
                break;

            default:
                break;
        }

        listener.onUnknownMessage(templateId);
        return -1;
    }
}

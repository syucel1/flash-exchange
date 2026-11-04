package com.flashexchange.engine.matching;

import java.util.Objects;

/**
 * Static reference data of a tradable instrument (currency pair).
 * <p>
 * Prices and quantities are fixed-point {@code long} values in instrument units. The engine does not interpret the
 * scale; it only enforces tick and lot multiples. For example with a price scale of 5, EUR/USD 1.08345 is
 * {@code 108345} and a tick size of {@code 1} means 0.00001.
 *
 * @param id          unique instrument id (1 .. 2^23-1).
 * @param symbol      display symbol, e.g. {@code EURUSD}.
 * @param tickSize    minimum price increment; every limit price must be a positive multiple of it.
 * @param lotSize     minimum quantity increment; every quantity must be a positive multiple of it.
 * @param minQuantity minimum order quantity.
 * @param maxQuantity maximum order quantity.
 * @param collarBps   market order price protection in basis points of the best opposite price at entry
 *                    (0 disables the collar).
 */
public record Instrument(
    int id,
    String symbol,
    long tickSize,
    long lotSize,
    long minQuantity,
    long maxQuantity,
    int collarBps)
{
    /** Largest instrument id: trade ids embed the instrument id in their upper bits. */
    public static final int MAX_ID = (1 << 23) - 1;

    /**
     * Validate the reference data.
     */
    public Instrument
    {
        Objects.requireNonNull(symbol, "symbol");
        if (id <= 0 || id > MAX_ID)
        {
            throw new IllegalArgumentException("instrument id must be in 1.." + MAX_ID + ": " + id);
        }
        if (tickSize <= 0 || lotSize <= 0)
        {
            throw new IllegalArgumentException("tickSize and lotSize must be positive: " + symbol);
        }
        if (minQuantity <= 0 || maxQuantity < minQuantity)
        {
            throw new IllegalArgumentException("invalid min/max quantity: " + symbol);
        }
        if (collarBps < 0 || collarBps > 10_000)
        {
            throw new IllegalArgumentException("collarBps must be in 0..10000: " + symbol);
        }
    }
}

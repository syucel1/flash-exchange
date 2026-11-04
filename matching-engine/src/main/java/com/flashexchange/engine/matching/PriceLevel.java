package com.flashexchange.engine.matching;

import com.flashexchange.protocol.Side;

/**
 * One price level of one side of a book: a FIFO queue of orders (intrusive doubly linked list) plus the running
 * aggregates published as L2 market data. Pooled; owned by a single matching thread.
 */
public final class PriceLevel
{
    Side side;
    long price;
    long totalQuantity;
    int orderCount;
    Order head;
    Order tail;

    void init(final Side side, final long price)
    {
        this.side = side;
        this.price = price;
        this.totalQuantity = 0;
        this.orderCount = 0;
        this.head = null;
        this.tail = null;
    }

    void reset()
    {
        init(null, 0);
    }

    /**
     * Append an order at the back of the queue (lowest time priority).
     *
     * @param order to append.
     */
    void append(final Order order)
    {
        order.level = this;
        order.next = null;
        order.prev = tail;
        if (tail == null)
        {
            head = order;
        }
        else
        {
            tail.next = order;
        }
        tail = order;
        totalQuantity += order.remainingQuantity();
        orderCount++;
    }

    /**
     * Unlink an order from anywhere in the queue. Its remaining quantity is removed from the level total.
     *
     * @param order to remove; must belong to this level.
     */
    void remove(final Order order)
    {
        final Order prev = order.prev;
        final Order next = order.next;
        if (prev == null)
        {
            head = next;
        }
        else
        {
            prev.next = next;
        }
        if (next == null)
        {
            tail = prev;
        }
        else
        {
            next.prev = prev;
        }
        totalQuantity -= order.remainingQuantity();
        orderCount--;
        order.prev = null;
        order.next = null;
        order.level = null;
    }

    /**
     * Reduce the level total after a fill of one of its orders.
     *
     * @param quantity filled.
     */
    void reduce(final long quantity)
    {
        totalQuantity -= quantity;
    }

    /**
     * Side of the level.
     *
     * @return side.
     */
    public Side side()
    {
        return side;
    }

    /**
     * Price of the level.
     *
     * @return price.
     */
    public long price()
    {
        return price;
    }

    /**
     * Total open quantity at the level.
     *
     * @return quantity.
     */
    public long totalQuantity()
    {
        return totalQuantity;
    }

    /**
     * Number of orders at the level.
     *
     * @return order count.
     */
    public int orderCount()
    {
        return orderCount;
    }

    /**
     * Order with the highest time priority.
     *
     * @return first order or {@code null} if empty.
     */
    public Order head()
    {
        return head;
    }

    /**
     * Whether the level has no orders.
     *
     * @return true if empty.
     */
    public boolean isEmpty()
    {
        return orderCount == 0;
    }
}

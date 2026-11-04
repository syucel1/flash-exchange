package com.flashexchange.engine.config;

/**
 * How the engine is reached by other components.
 */
public enum TransportMode
{
    /** Same box: Aeron IPC over shared memory. */
    IPC,

    /** Across the network: Aeron reliable UDP (unicast commands, multi-destination-cast output). */
    UDP
}

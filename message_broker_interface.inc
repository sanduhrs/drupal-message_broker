<?php

/**
 * Represents the message broker, that may be implemented via AQMP or any 
 * other protocol.
 */
interface IMessageBroker {
    
    /**
     * Sends a message to the message broker. 
     * @param string $body
     * @param string $destination
     * @param array $options
     */
    function sendMessage($body, $destination, array $options = array());
}

<?php

class DummyMessageBroker implements \IMessageBroker {
    
    protected $consumers;
    
    protected $queues;
    
    protected $exchanges;
    
    public function __construct($config, $consumers) {
        $this->exchanges = $config['exchanges'];
        $this->queues = $config['queues'];
        $this->consumers = $consumers;
    }
    
    protected function findExchangeByName($exchangeName) {
        foreach ($this->exchanges as $exchange) {
            if ($exchange['name'] == $exchangeName) {
                return $exchange;
            }
        }
        
        return false;
    }

    /**
     * Sends a message to the message broker. 
     * @param mixed $body
     * @param string $routingKey
     * @param string $exchange
     */
    public function sendMessage($body, $destination, array $options = array()) {       
        $exchange = $this->findExchangeByName($destination);
        
        if ($exchange === false) {
            throw new MessageBrokerException('The specified exchange name is not known, check your json configuration file!');
        }
        
        drupal_set_message('Message sent to exchange "' . $destination . '" with routing key "' . $options['routing_key'] . '"');
        
        $queues = $this->getQueuesForMessage($options['routing_key'], $exchange['name'], $exchange['type']);
        
        foreach ($queues as $queueName) {
            foreach ($this->consumers as $consumerName => $consumer) {
                if ($consumer['queue'] == $queueName) {
                    $this->callConsumer($body, $consumer, $consumerName);
                }
            }
        }
    }
    
    /**
     * Consume message using the specified consumer.
     * @param type $consumer 
     */
    protected function callConsumer($body, $consumer, $consumerName) {
        $message = new stdClass();
        $message->body = $body;
        $acked = false;
        
        try {
            drupal_set_message('Start consuming a message with "' . $consumerName . '"');
            call_user_func($consumer['callback'], $message, function() use(&$acked) {
                $acked = true;
            });
        } catch (Exception $e) {
            watchdog(WATCHDOG_ERROR, 'An exception was raised while consuming a message by the "' . $consumerName . '". Message: ' . print_r($body, 1));
            watchdog_exception('dummy_message_broker', $e);
        }
        
        if (!$acked) {
            watchdog(WATCHDOG_WARNING, 'A message was not processed successfully, because it was not acked by the consumer "' . $consumerName . '"! Message: ' . print_r($body, 1));
            drupal_set_message('A message was not processed successfully, it was not acked by the consumer "' . $consumerName . '"!', 'warning');
        } else {
            drupal_set_message('Message was processed successfully by consumer "' . $consumerName . '".');
        }
    }
    
    /**
     * Matches a routing key with a given pattern.
     * @param string $routingKey
     * @param string $pattern
     * @return boolean if routing key matches the given pattern
     * @throws MessageBrokerException 
     */
    protected function matchRoutingKey($routingKey, $pattern) {
        if ($pattern == '#') {
            return true;
        }
        
        $routingElements = explode('.', $routingKey);
        $patternElements = explode('.', $pattern);
        $matches = true;
        
        if (empty($routingElements)) {
            return false;
        }

        foreach ($patternElements as $pos => $pattern) {
            if ($pattern == '#') {
                if ($pos == count($patternElements) - 1) {
                    // we allow the hash at the end, other positions are not supported at the moment
                    $matches = $matches && true; // the hash stands for anything
                    break;
                } else {
                    throw new MessageBrokerException('Pattern matching with "#" hashes within the pattern is not implemented yet!');
                }
            }
            
            $matches = $matches && ($pattern == '*' || $pattern == $routingElements[$pos]);
        }
        
        return $matches;
    }
    
    /**
     * Finds all queues that are bound to the specified exchange and message routing key.
     * @param string $routingKey
     * @param string $fromExchange
     * @param string $exchangeType
     * @return array of queue names
     * @throws MessageBrokerException 
     */
    protected function getQueuesForMessage($routingKey, $fromExchange, $exchangeType) {
        $queues = array();
        
        if ($exchangeType == 'direct') {
            // search all queues which are bound to the exchange and have the same routing key
            foreach ($this->queues as $queue) {
                foreach ($queue['bindings'] as $binding) {
                    if ($binding['exchange'] == $fromExchange && $routingKey == $binding['routing_key']) {
                        $queues[] = $queue['name'];
                    }
                }
            }
        } else if ($exchangeType == 'topic') {
            // search all queues which are bound to the exchange
            foreach ($this->queues as $queue) {
                foreach ($queue['bindings'] as $binding) {
                    if ($binding['exchange'] == $fromExchange && $this->matchRoutingKey($routingKey, $binding['routing_key'])) {
                        $queues[] = $queue['name'];
                    }
                }
            }
        } else if ($exchangeType == 'fanout') {
            // search all queues which are bound to the exchange
            foreach ($this->queues as $queue) {
                foreach ($queue['bindings'] as $binding) {
                    if ($binding['exchange'] == $fromExchange) {
                        $queues[] = $queue['name'];
                    }
                }
            }
        } else if ($exchangeType == 'headers') {
            throw new MessageBrokerException('Exchange type "headers" is not implemented!');
        } else {
            throw new MessageBrokerException('Uknown exchange type "' . $exchangeType . '" of exchange with name "' . $fromExchange . '"');
        }
        
        return $queues;
    }
   
}
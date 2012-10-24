<?php

/**
 * Implementation of the IMessagebroker interface to integrate
 * an external AMQP message broker.
 */
class AmqpMessageBroker implements \IMessageBroker {

    /** @var \PhpAmqpLib\Connection\AMQPConnection  */
    protected $connection;

    /** @var \PhpAmqpLib\Channel\AMQPChannel */
    protected $channel;

    /** @var array */
    protected $consumers;

    /**
     * Creates an AmqpMessageBroker instance. Specify an array with infos
     * about host, vhost and the credentials:
     * array('host' => 'localhost', 'port' => 80, 'vhost' => '/', 'username' => '?',
     *       'password' => '?')
     * @param array $params
     * @param array $consumers
     */
    public function __construct(array $params, array $consumers) {
        $this->init($params);

        $this->consumers = $consumers;
    }

    protected function init(array $params) {
        try {
            $this->connection = new \PhpAmqpLib\Connection\AMQPConnection($params['host'], $params['port'],
                    $params['username'], $params['password'], $params['vhost']);
            $this->channel = $this->connection->channel(null);

            $this->declareExchanges($params['config']['exchanges']);
            $this->declareQueues($params['config']['queues']);
        } catch (Exception $e) {
            throw new MessageBrokerException('Connecting to the AMQP server ' . $params['host'] . ' on port ' . $params['port'] . ' failed: ' . $e->getMessage());
        }
    }

    /**
     * Declares all exchanges.
     * @param type $exchanges 
     */
    protected function declareExchanges($exchanges) {
        foreach ($exchanges as $exchange) {
            if (!isset($exchange['passive'])) {
                $exchange['passive'] = FALSE;
            }
            
            if (!isset($exchange['durable'])) {
                $exchange['durable'] = FALSE;
            }
            
            if (!isset($exchange['auto_delete'])) {
                $exchange['auto_delete'] = TRUE;
            }
            
            if (!isset($exchange['internal'])) {
                $exchange['internal'] = FALSE;
            }
            
            if (!isset($exchange['nowait'])) {
                $exchange['nowait'] = FALSE;
            }
            
            $this->channel->exchange_declare($exchange['name'], $exchange['type'], 
                $exchange['passive'], $exchange['durable'], $exchange['auto_delete'], 
                $exchange['internal'], $exchange['nowait']);
        }
    }

    /**
     * Declares all queues and its bindings.
     * @param type $queues 
     */
    protected function declareQueues($queues) {
        if (!empty($queues)) {
            foreach ($queues as $queue) {
                if (!isset($queue['passive'])) {
                    $queue['passive'] = FALSE;
                }

                if (!isset($queue['durable'])) {
                    $queue['durable'] = FALSE;
                }

                if (!isset($queue['nowait'])) {
                    $queue['nowait'] = FALSE;
                }

                if (!isset($queue['exclusive'])) {
                    $queue['exclusive'] = FALSE;
                }

                if (!isset($queue['auto_delete'])) {
                    $queue['auto_delete'] = TRUE;
                }

                $this->channel->queue_declare($queue['name'], $queue['passive'], $queue['durable'], 
                    $queue['exclusive'], $queue['auto_delete'], $queue['nowait']);

                if (!empty($queue['bindings'])) {
                    foreach ($queue['bindings'] as $binding) {
                        $this->channel->queue_bind($queue['name'], $binding['exchange'], $binding['routing_key']);
                    }
                }
            }
        }
    }

    /**
     * Gets the parameters used when publishing messages.
     * @param array $options
     * @return array
     */
    protected function getMessageParams(array $options) {
        if (!isset($options['content_type'])) {
            $options['content_type'] = 'text/plain';
        }
        
        if (!isset($options['delivery_mode'])) {
            $options['delivery_mode'] = 2; // durable message
        }

        return $options;
    }

    /**
     * Sends a message to the message broker. Specify the routing within the third
     * options parameter, like so array('routing_key' => 'your.key').
     * You can pass more message options:
     * - delivery_mode (default value = '2', which means persistent)
     * - content_type (default = 'text/plain')
     * - anything you want
     * @param string $body
     * @param string $destination
     * @param array $options
     */
    public function sendMessage($body, $destination, array $options = array()) {
        if (!is_string($body)) {
            throw new InvalidArgumentException('The message body must be a string! Serialize your data to json, xml or anything you prefer.');
        }
        
        $routingKey = "";
        
        if (isset($options['routing_key'])) {
            $routingKey = $options['routing_key'];
            unset($options['routing_key']);
        }

        $msgOptions = $this->getMessageParams($options);

        $msg = new \PhpAmqpLib\Message\AMQPMessage($body, $msgOptions);

        $this->channel->basic_publish($msg, $destination, $routingKey);
    }

    /**
     * Checks whether a specific consumer exists.
     * @param string $consumerName
     * @return bool
     */
    public function consumerExists($consumerName) {
        return !empty($this->consumers[$consumerName]);
    }

    /**
     * Consumes messages with all or only the specified consumer.
     * @param string $consumerName
     * @param bool $onlyOnce only consume the messages currently in the pipeline, dont wait for new ones
     * @param IConsumerLogger $logger
     * @return int count consumed messages
     */
    public function consumeMessages($consumerName, $countMessagesToProcess, IConsumerLogger $logger) {
        $consumers = array();

        if (!empty($consumerName)) {
            if ($this->consumerExists($consumerName)) {
                $consumers[$consumerName] = $this->consumers[$consumerName];
            } else {
                return $logger->logError('The consumer "' . $consumerName . '" does not exist!');
            }
        } else {
            $consumers = $this->consumers;
        }

        if (empty($consumers)) {
            return $logger->logInfo('There are no consumers available');
        }

        $countMessages = 0;

        foreach ($consumers as $name => $consumerInfos) {
            $this->channel->basic_consume($consumerInfos['queue'], $name, FALSE, FALSE, FALSE, FALSE, function($message) use($consumerInfos, $logger, $name, &$countMessages) {
                    $logger->logInfo('Process message with consumer "' . $name . '"');
                    $acked = FALSE;

                    try {
                        call_user_func($consumerInfos['callback'], $message, function() use ($message, $logger, $name, &$countMessages, &$acked) {
                                $message->delivery_info['channel']->basic_ack($message->delivery_info['delivery_tag']);
                                $acked = TRUE;
                                $countMessages++;
                                $logger->logInfo("Message was processed successfully\n");
                            });
                    } catch (InvalidMessageException $e) {
                        if (!$acked) {
                            $message->delivery_info['channel']->basic_ack($message->delivery_info['delivery_tag']);
                            $acked = TRUE;
                            $logger->logInfo("Invalid message was acked\n");
                        }
                        
                        $this->handleInvalidMessage($message, $consumerInfos);
                    } catch (CriticalErrorException $e) {
                        $logger->logError("An critical exception was raised while consuming a message. Consumer will quit. Exception of type \"" . get_class($e) . "\": " . $e->getMessage() . "\nstacktrace: " . $e->getTraceAsString() . "\n");
                        watchdog('amqp_message_broker', 'An critical exception was raised while consuming a message by the "' . $consumerName . '". Message: ' . print_r($message, 1));
                        watchdog_exception('amqp_message_broker', $e);
                        
                        die();
                    } catch (Exception $e) {
                        $logger->logError("An exception was raised while consuming a message. Exception of type \"" . get_class($e) . "\": " . $e->getMessage() . "\nstacktrace: " . $e->getTraceAsString() . "\n");
                        watchdog('amqp_message_broker', 'An exception was raised while consuming a message by the "' . $consumerName . '". Message: ' . print_r($message, 1));
                        watchdog_exception('amqp_message_broker', $e);
                    }

                    if (!$acked) {
                        $logger->logError("Message was not processed successfully, another consumer may try later again\n");
                    }
                });
        }

        while (count($this->channel->callbacks)) {
            $this->channel->wait();

            if ($countMessagesToProcess > 0 && $countMessages >= $countMessagesToProcess) {
                break;
            }
        }

        return $countMessages;
    }

    /**
     * Handles an invalid message.
     * @param mixed $message
     * @param array $consumerInfos
     */
    protected function handleInvalidMessage($message, $consumerInfos) {
        if (isset($consumerInfos['invalidMessageHandler'])) {
            call_user_func($consumerInfos['invalidMessageHandler'], $message);
        } else {
            watchdog('amqp_message_broker', 'Invalid message, body:' . $message->body);
        }
    }
}
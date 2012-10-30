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
	 *
     * @param array $params
     * @param array $consumers
     */
    public function __construct(array $params, array $consumers) {
        $this->init($params);

        $this->consumers = $consumers;
    }

	/**
	 * Connects to the message broker and declares all exchanges and queues.
	 *
	 * @param array $params
	 */
    protected function init(array $params) {
        try {
            $this->connection = new \PhpAmqpLib\Connection\AMQPConnection($params['host'], $params['port'],
                    $params['username'], $params['password'], $params['vhost']);
            $this->channel = $this->connection->channel(NULL);

            $this->declareExchanges($params['config']['exchanges']);
            $this->declareQueues($params['config']['queues']);
        } catch (Exception $e) {
            throw new MessageBrokerException('Connecting to the AMQP server ' . $params['host'] . ' on port ' . $params['port'] . ' failed: ' . $e->getMessage());
        }
    }

    /**
     * Declares all exchanges.
	 *
     * @param array $exchanges 
     */
    protected function declareExchanges(array $exchanges = array()) {
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
	 *
     * @param array $queues 
     */
    protected function declareQueues(array $queues = array()) {
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
	 *
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
	 *
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
	 *
     * @param string $consumer_name
     * @return bool
     */
    public function consumerExists($consumer_name) {
        return !empty($this->consumers[$consumer_name]);
    }

    /**
     * Consumes messages with all or only the specified consumer.
	 *
     * @param string $consumer_name
     * @param int count_messages_to_process count of messages to consume
     * @param IConsumerLogger $logger
     * @return int count consumed messages
     */
    public function consumeMessages($consumer_name, $count_messages_to_process, IConsumerLogger $logger) {
        $consumers = array();

        if (!empty($consumer_name)) {
            if ($this->consumerExists($consumer_name)) {
                $consumers[$consumer_name] = $this->consumers[$consumer_name];
            } else {
                return $logger->logError('The consumer "' . $consumer_name . '" does not exist!');
            }
        } else {
            $consumers = $this->consumers;
        }

        if (empty($consumers)) {
            return $logger->logInfo('There are no consumers available');
        }

        $count_messages = 0;

        foreach ($consumers as $name => $consumerInfos) {
            $this->channel->basic_consume($consumerInfos['queue'], $name, FALSE, FALSE, FALSE, FALSE, function($message) use($consumerInfos, $logger, $name, &$count_messages) {
                    $logger->logInfo('Process message with consumer "' . $name . '"');
                    $acked = FALSE;

                    try {
                        call_user_func($consumerInfos['callback'], $message, function() use ($message, $logger, $name, &$count_messages, &$acked) {
                                $message->delivery_info['channel']->basic_ack($message->delivery_info['delivery_tag']);
                                $acked = TRUE;
                                $count_messages++;
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
                        watchdog('amqp_message_broker', 'An critical exception was raised while consuming a message by the "' . $consumer_name . '". Message: ' . print_r($message, 1));
                        watchdog_exception('amqp_message_broker', $e);
                        
                        die(1);
                    } catch (Exception $e) {
                        $logger->logError("An exception was raised while consuming a message. Exception of type \"" . get_class($e) . "\": " . $e->getMessage() . "\nstacktrace: " . $e->getTraceAsString() . "\n");
                        watchdog('amqp_message_broker', 'An exception was raised while consuming a message by the "' . $consumer_name . '". Message: ' . print_r($message, 1));
                        watchdog_exception('amqp_message_broker', $e);
                    }

                    if (!$acked) {
                        $logger->logError("Message was not processed successfully, another consumer may try later again\n");
                    }
                });
        }

        while (count($this->channel->callbacks)) {
            $this->channel->wait();

            if ($count_messages_to_process > 0 && $count_messages >= $count_messages_to_process) {
                break;
            }
        }

        return $count_messages;
    }

    /**
     * Handles an invalid message.
	 *
     * @param mixed $message
     * @param array $consumer_infos
     */
    protected function handleInvalidMessage($message, $consumer_infos) {
        if (isset($consumer_infos['invalidMessageHandler'])) {
            call_user_func($consumer_infos['invalidMessageHandler'], $message);
        } else {
            watchdog('amqp_message_broker', 'Invalid message, body:' . $message->body);
        }
    }
}

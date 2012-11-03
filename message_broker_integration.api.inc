<?php

/**
 * @file
 * API documentation for the message broker module.
 */

/**
 * Registers consumers in the message broker. Specify the queue you want to consume and
 * your callback. 
 * The given callback is called with two parameters: body and ack, a callable method you
 * use to ack the incoming message.
 * Optional, you can specify an invalidMessageHandler that reveives invalid messages.
 *
 * @param string $self_name
 * @return array consumers
 */
function hook_message_broker_consumers($self_name) {
  $consumers = array();

  $consumers['foo_consumer_' . $self_name] = array('queue' => 'foo' . $self_name,
      'callback' => 'mymodule_consume_message', 'invalidMessageHandler' => 'mymodule_log_invalid_message');

  return $consumers;
}

/**
 * Consumes one message.
 *
 * @param mixed $message
 * @param callable $ack
 */
function mymodule_consume_message($message, $ack) {
  // Process and ack message
  $ack();
}

/**
 * Logs invalid messages to watchdog. This callback is executed whenever a consumer throws
 * an InvalidMessageException. The message broker module catches these exceptions, acks
 * the message and allows to handle the invalid data on your own.
 *
 * @param mixed $msg
 */
function mymodule_log_invalid_message($msg) {
  // Log message for instance to a file
}

/**
 * Registers a message broker integration implementation. Describe the implementation via
 * a title, description and a factoryMethod. The latter one shall return an instance that implements the 
 * IMessageBroker interface.
 *
 * @return array
 */
function hook_message_broker_integration_implementation() {
  $implementations = array();

  $implementations['foo'] = array('title' => t('Foo'), 'factoryMethod' => 'mymodule_create_implementation',
      'description' => t('A message broker implementation'));

  return $implementations;
}

/**
 * Creates an instanceof the IMessageBroker.
 *
 * @param array $consumers
 * @param string $self_name
 */
function mymodule_create_implementation($consumers, $self_name) {
  // return instanceof IMessageBroker
}

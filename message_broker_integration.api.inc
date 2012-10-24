<?php

/**
 * Registers consumers in the message broker. Specify the queue you want to consume and
 * your callback. 
 * The given callback is called with two parameters: body and ack, a callable method you
 * use to ack the incoming message.
 * Optional, you can specify a invalidMessageHandler that reveives invalid messages.
 * @param string $selfName
 * @return array 
 */
function hook_message_broker_consumers($selfName) {
    $consumers = array();
    
    $consumers['foo_consumer_' . $selfName] = array('queue' => 'foo' . $selfName, 
        'callback' => 'mymodule_consume_message', 'invalidMessageHandler' => 'mymodule_log_invalid_message');
    
    return $consumers;
}

/**
 * Consumes one message.
 * @param mixed $message
 * @param callable $ack
 */
function mymodule_consume_message($message, $ack) {
    // process and ack message
    $ack();
}

/**
 * Logs invalid messages to watchdog. This callback is executed whenever a consumer throws
 * an InvalidMessageException. The message broker module catches these exceptions, acks
 * the message and allows to handle the invalid data on your own.
 * @param mixed $msg
 */
function mymodule_log_invalid_message($msg) {
    // log message for instance to a file
}

/**
 * Registers a message broker integration implementation. Describe the implementation via
 * a title, description and a factoryMethod. The latter one shall return an instance that implements the 
 * IMessageBroker interface.
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
 * @param array $consumers
 * @param string $selfName
 */
function mymodule_create_implementation($consumers, $selfName) {
    // return instanceof IMessageBroker
}
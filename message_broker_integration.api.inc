<?php

/**
 * Registers a message broker integration implementation. Describe the implementation via
 * a title, description and a factoryMethod. The latter one shall return an instance that implements the 
 * IMessageBroker interface.
 * @return array
 */
function hook_message_broker_integration_implementation() {
    $implementations = array();
    
    $implementations['foo'] = array('title' => t('Foo'), 'factoryMethod' => 'myImplCreateFunc', 'description' => t('A message broker implementation'));
    
    return $implementations;
}

function myImplCreateFunc($consumers, $selfName) {
    // return instanceof IMessageBroker
}

/**
 * Registers consumers in the message broker. Specify the queue you want to consume and
 * your callback.
 * The given callback is called with two parameters: body and ack, a callable method you
 * use to ack the incoming message.
 * @param string $selfName
 * @return array 
 */
function hook_message_broker_consumers($selfName) {
    $consumers = array();
    
    $consumers['foo_consumer_' . $selfName] = array('queue' => 'foo' . $selfName, 'callback' => 'myConsumerFunc');
    
    return $consumers;
}

function myConsumerFunc($message, $ack) {
    // do something with message
    $ack();
}
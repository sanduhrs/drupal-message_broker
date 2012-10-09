<?php

/**
 * Implements the hook_drush_command that registers drush commands.
 * @return array 
 */
function message_broker_amqp_integration_drush_command() {
    $items = array();
    
    $items['consume-amqp-messages'] = array(
        'description' => 'Consumes AMQP messages using all or a specific consumer',
        'arguments' => array(
            'consumer' => 'The consumer to start, if not specified, the command will start all consumers'
        ),
        'options' => array(
            'count-messages' => 'Consume the given count of messages, blocks until finished'
        ),
        'bootstrap' => DRUSH_BOOTSTRAP_FULL,
        'aliases' => array('cons'),
        'examples' => array(
            'drush consume-amqp-messages helloWorldToEveryone --count-messages=10' => 'Consumes 10 messages for the consumer "helloWorldToEveryone"',
            'drush cons' => 'Starts alls consumers and consumes all messages forever'
        )
    );
    
    return $items;
}

/**
 * Implements the drush command "consume-amqp-messages".
 * @param string $consumerName
 * @return boolean 
 */
function drush_message_broker_amqp_integration_consume_amqp_messages($consumerName = '') {
    try {
        $broker = message_broker_integration_get();
    } catch (MessageBrokerException $e) {
        return drush_print(dt('Error: The message broker integration is not properly configured. Wrong credentials or host/port? Please visit admin/config/system/message_broker_integration, details: ') . $e->getMessage());
    }
    
    if (!$broker instanceof AmqpMessageBroker) {
        return drush_print(dt('Error: This drush command only works if you set the message broker configuration to AMQP. Please visit admin/config/system/message_broker_integration'));
    }
    
    $messageBrokerHost = variable_get('message_broker_amqp_integration_host', 'localhost');
    $messageBrokerPort = variable_get('message_broker_amqp_integration_port', '5672');
    drush_print(dt('Connected successfully to the AMQP message broker at ') . $messageBrokerHost . ':' . $messageBrokerPort);
    
    if (empty($consumerName)) {
        drush_print(dt('Starting all consumers ...'));
    } else {
        drush_print(dt('Starting the consumer "' . $consumerName . '" ...'));
    }
    
    $broker->consumeMessages($consumerName, (int)drush_get_option('count-messages', null), new DrushConsumerLogger());
    
    return TRUE;
}
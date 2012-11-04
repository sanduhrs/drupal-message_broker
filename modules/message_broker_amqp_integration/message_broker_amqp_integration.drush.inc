<?php

/**
 * @file
 * Drush command for consuming AMQP messages.
 */

/**
 * Implements hook_drush_command() for registering the consuming command.
 *
 * @return array
 *   Registered commands.
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
      'drush cons' => 'Starts alls consumers and consumes all messages forever',
    ),
  );

  return $items;
}

/**
 * Implements the drush command "consume-amqp-messages".
 *
 * @param string $consumer_name
 *   Name of consumer.
 *
 * @return boolean 
 */
function drush_message_broker_amqp_integration_consume_amqp_messages($consumer_name = '') {
  try {
    $broker = message_broker_integration_get();
  } catch (MessageBrokerException $e) {
    return drush_print(dt('Error: The message broker integration is not properly configured. Wrong credentials or host/port? Please visit admin/config/system/message_broker_integration, details: ') . $e->getMessage());
  }

  if (!$broker instanceof AmqpMessageBroker) {
    return drush_print(dt('Error: This drush command only works if you set the message broker configuration to AMQP. Please visit admin/config/system/message_broker_integration'));
  }

  $message_broker_host = variable_get('message_broker_amqp_integration_host', 'localhost');
  $message_broker_port = variable_get('message_broker_amqp_integration_port', '5672');
  drush_print(dt('Connected successfully to the AMQP message broker at ') . $message_broker_host . ':' . $message_broker_port);

  if (empty($consumer_name)) {
    drush_print(dt('Starting all consumers ...'));
  } else {
    drush_print(dt('Starting the consumer "' . $consumer_name . '" ...'));
  }

  $broker->consumeMessages($consumer_name, (int) drush_get_option('count-messages',
          NULL), new DrushConsumerLogger());

  return TRUE;
}

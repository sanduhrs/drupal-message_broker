<?php

/**
 * DrushConsumerLogger that prints messages to the console.
 */
class DrushConsumerLogger implements IConsumerLogger {

  function logInfo($message) {
    drush_print($message);
  }

  function logError($message) {
    return drush_print('Error (' . date('d.m.Y H:i') . '): ' . $message);
  }

}

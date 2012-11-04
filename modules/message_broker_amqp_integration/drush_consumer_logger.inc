<?php

/**
 * @file
 * Drush implementation of IConsumerLogger interface.
 */

/**
 * DrushConsumerLogger that prints messages to the console.
 */
class DrushConsumerLogger implements IConsumerLogger {

  /**
   * Logs an info message.
   *
   * @param string $message
   */
  public function logInfo($message) {
    drush_print($message);
  }

  /**
   * Logs an error message.
   *
   * @param string $message
   */
  public function logError($message) {
    return drush_print('Error (' . date('d.m.Y H:i') . '): ' . $message);
  }

}

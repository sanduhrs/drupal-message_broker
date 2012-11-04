<?php

/**
 * @file
 * Interface for a Logger that reports consuming infos.
 */

/**
 * Represents a logger that outputs info and error strings while consuming
 * message broker messages. 
 */
interface IConsumerLogger {

  /**
   * Logs an info message.
   *
   * @param string $message
   */
  public function logInfo($message);

  /**
   * Logs an error message.
   *
   * @param string $message
   */
  public function logError($message);
}

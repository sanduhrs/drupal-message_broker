<?php

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
  function logInfo($message);

  /**
   * Logs an error message.
   *
   * @param string $message
   */
  function logError($message);
}

<?php

/**
 * Represents a logger that outputs info and error strings while consuming
 * message broker messages. 
 */
interface IConsumerLogger {
    
    function logInfo($message);
    
    function logError($message);
}
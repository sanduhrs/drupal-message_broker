<?php

/**
 * General Exception
 */
class MessageBrokerException extends Exception {
    
}

/**
 * Represents a critical error. The application is not able
 * to proceed at this state and should be restarted.
 * The message broker infrastructure catches these exceptions
 * and termines the process.
 */
class CriticalErrorException extends MessageBrokerException {
    
}

/**
 * The Message is corrupt
 */
class InvalidMessageException extends MessageBrokerException {
    
}

/**
 * The Content of the Message can't be handled.
 */
class InvalidMessageBodyException extends InvalidMessageException {
    
}
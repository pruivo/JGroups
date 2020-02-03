package org.jgroups.util;

import java.util.Iterator;

import org.jgroups.Message;

/**
 * //TODO document this!
 *
 * @author Pedro Ruivo
 * @since 9.0
 */
public interface MessageIterator extends Iterator<Message> {

   void replace(Message msg);

}

/*
 * JBoss, Home of Professional Open Source.
 * Copyright (c) 2017, Red Hat, Inc., and individual contributors
 * as indicated by the @author tags. See the copyright.txt file in the
 * distribution for a full listing of individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */
package org.xnio.nio;

import java.util.Collection;
import java.util.Deque;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

/**
 * @author <a href="mailto:carlo@nerdnet.nl">Carlo de Wolf</a>
 */
class CheckingConcurrentDeque<E> implements Deque<E> {
    private final AtomicReference<Thread> currentThread = new AtomicReference<>();
    private final Deque<E> delegate;

    public CheckingConcurrentDeque(final Deque<E> delegate) {
        this.delegate = delegate;
    }

    private <V> V call(final Supplier<V> func) {
        final Thread prev = currentThread.getAndSet(Thread.currentThread());
        if (prev != null)
            throw new IllegalStateException("Thread " + prev + " was already in");
        try {
            return func.get();
        } finally {
            final Thread discard = currentThread.getAndSet(null);
            if (!discard.equals(Thread.currentThread()))
                throw new IllegalStateException("Thread " + discard + " got hold of this");
        }
    }

    @Override
    public void addFirst(final E e) {
        throw new RuntimeException("NYI: org.xnio.nio.CheckingConcurrentDeque.addFirst");
    }

    @Override
    public void addLast(final E e) {
        throw new RuntimeException("NYI: org.xnio.nio.CheckingConcurrentDeque.addLast");
    }

    @Override
    public boolean offerFirst(final E e) {
        throw new RuntimeException("NYI: org.xnio.nio.CheckingConcurrentDeque.offerFirst");
    }

    @Override
    public boolean offerLast(final E e) {
        throw new RuntimeException("NYI: org.xnio.nio.CheckingConcurrentDeque.offerLast");
    }

    @Override
    public E removeFirst() {
        throw new RuntimeException("NYI: org.xnio.nio.CheckingConcurrentDeque.removeFirst");
    }

    @Override
    public E removeLast() {
        throw new RuntimeException("NYI: org.xnio.nio.CheckingConcurrentDeque.removeLast");
    }

    @Override
    public E pollFirst() {
        throw new RuntimeException("NYI: org.xnio.nio.CheckingConcurrentDeque.pollFirst");
    }

    @Override
    public E pollLast() {
        throw new RuntimeException("NYI: org.xnio.nio.CheckingConcurrentDeque.pollLast");
    }

    @Override
    public E getFirst() {
        throw new RuntimeException("NYI: org.xnio.nio.CheckingConcurrentDeque.getFirst");
    }

    @Override
    public E getLast() {
        throw new RuntimeException("NYI: org.xnio.nio.CheckingConcurrentDeque.getLast");
    }

    @Override
    public E peekFirst() {
        throw new RuntimeException("NYI: org.xnio.nio.CheckingConcurrentDeque.peekFirst");
    }

    @Override
    public E peekLast() {
        throw new RuntimeException("NYI: org.xnio.nio.CheckingConcurrentDeque.peekLast");
    }

    @Override
    public boolean removeFirstOccurrence(final Object o) {
        throw new RuntimeException("NYI: org.xnio.nio.CheckingConcurrentDeque.removeFirstOccurrence");
    }

    @Override
    public boolean removeLastOccurrence(final Object o) {
        throw new RuntimeException("NYI: org.xnio.nio.CheckingConcurrentDeque.removeLastOccurrence");
    }

    @Override
    public boolean add(final E e) {
        return call(() -> delegate.add(e));
    }

    @Override
    public boolean offer(final E e) {
        throw new RuntimeException("NYI: org.xnio.nio.CheckingConcurrentDeque.offer");
    }

    @Override
    public E remove() {
        throw new RuntimeException("NYI: org.xnio.nio.CheckingConcurrentDeque.remove");
    }

    @Override
    public E poll() {
        return call(() -> delegate.poll());
    }

    @Override
    public E element() {
        throw new RuntimeException("NYI: org.xnio.nio.CheckingConcurrentDeque.element");
    }

    @Override
    public E peek() {
        return call(() -> delegate.peek());
    }

    @Override
    public void push(final E e) {
        throw new RuntimeException("NYI: org.xnio.nio.CheckingConcurrentDeque.push");
    }

    @Override
    public E pop() {
        throw new RuntimeException("NYI: org.xnio.nio.CheckingConcurrentDeque.pop");
    }

    @Override
    public boolean remove(final Object o) {
        throw new RuntimeException("NYI: org.xnio.nio.CheckingConcurrentDeque.remove");
    }

    @Override
    public boolean containsAll(final Collection<?> c) {
        throw new RuntimeException("NYI: org.xnio.nio.CheckingConcurrentDeque.containsAll");
    }

    @Override
    public boolean addAll(final Collection<? extends E> c) {
        throw new RuntimeException("NYI: org.xnio.nio.CheckingConcurrentDeque.addAll");
    }

    @Override
    public boolean removeAll(final Collection<?> c) {
        throw new RuntimeException("NYI: org.xnio.nio.CheckingConcurrentDeque.removeAll");
    }

    @Override
    public boolean retainAll(final Collection<?> c) {
        throw new RuntimeException("NYI: org.xnio.nio.CheckingConcurrentDeque.retainAll");
    }

    @Override
    public void clear() {
        throw new RuntimeException("NYI: org.xnio.nio.CheckingConcurrentDeque.clear");
    }

    @Override
    public boolean contains(final Object o) {
        throw new RuntimeException("NYI: org.xnio.nio.CheckingConcurrentDeque.contains");
    }

    @Override
    public int size() {
        throw new RuntimeException("NYI: org.xnio.nio.CheckingConcurrentDeque.size");
    }

    @Override
    public boolean isEmpty() {
        return call(() -> delegate.isEmpty());
    }

    @Override
    public Iterator<E> iterator() {
        throw new RuntimeException("NYI: org.xnio.nio.CheckingConcurrentDeque.iterator");
    }

    @Override
    public Object[] toArray() {
        throw new RuntimeException("NYI: org.xnio.nio.CheckingConcurrentDeque.toArray");
    }

    @Override
    public <T> T[] toArray(final T[] a) {
        throw new RuntimeException("NYI: org.xnio.nio.CheckingConcurrentDeque.toArray");
    }

    @Override
    public Iterator<E> descendingIterator() {
        throw new RuntimeException("NYI: org.xnio.nio.CheckingConcurrentDeque.descendingIterator");
    }
}

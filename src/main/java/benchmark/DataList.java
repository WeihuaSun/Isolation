package benchmark;

/*
 * DataList - A double linked list of Terminal Data objects.
 *
 */
public class DataList<T extends TData<T>> {
    private T head;
    private T tail;

    public DataList() {
        this.head = null;
        this.tail = null;
    }

    public void append(T data) {
        if (head == null) {
            head = tail = data;
            data.term_left = null;
            data.term_right = null;
        } else {
            tail.term_right = data;
            data.term_left = tail;
            data.term_right = null;
            tail = data;
        }
    }

    public void prepend(T data) {
        if (head == null) {
            head = tail = data;
            data.term_left = null;
            data.term_right = null;
        } else {
            head.term_left = data;
            data.term_left = null;
            data.term_right = head;
            head = data;
        }
    }

    public void remove(T data) {
        if (head == data)
            head = data.term_right;
        else
            data.term_left.term_right = data.term_right;
        if (tail == data)
            tail = data.term_left;
        else
            data.term_right.term_left = data.term_left;

        data.term_left = null;
        data.term_right = null;
    }

    public T first() {
        return head;
    }

    public T last() {
        return tail;
    }

    public T next(T data) {
        return data.term_right;
    }

    public T prev(T data) {
        return data.term_left;
    }

    public void truncate() {
        T next;

        while (head != null) {
            next = head.term_right;
            head.term_left = null;
            head.term_right = null;
            head = next;
        }
        tail = null;
    }
}

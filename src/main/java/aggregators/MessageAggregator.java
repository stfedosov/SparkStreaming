package aggregators;

import deserialization.Message;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;

/**
 * @author sfedosov on 5/9/19.
 */
public class MessageAggregator implements Serializable {

    private int clicks;
    private int views;
    private final Set<Integer> seenCategories;

    public MessageAggregator() {
        this.seenCategories = new HashSet<>();
    }

    public int getClicks() {
        return clicks;
    }

    public int getViews() {
        return views;
    }

    public Set<Integer> seenCategories() {
        return seenCategories;
    }

    public void mergeAggregators(MessageAggregator other) {
        this.clicks += other.getClicks();
        this.views += other.getViews();
        this.seenCategories.addAll(other.seenCategories());
    }

    public void addMessage(Message message) {
        this.clicks += message.getType() == Message.TYPE.click ? 1 : 0;
        this.views += message.getType() == Message.TYPE.view ? 1 : 0;
        this.seenCategories.add(message.getCategoryId());
    }
}

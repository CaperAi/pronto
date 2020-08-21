package caper.pronto;

public class DeletionFailed extends RuntimeException {
    public DeletionFailed(String message) {
        super(message);
    }
}

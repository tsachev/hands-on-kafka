package demo.producer;

final class Thumbnail {
    final String title;
    final String url;
    final byte[] content;

    public Thumbnail(String title, String url, byte[] content) {
        this.title = title;
        this.url = url;
        this.content = content;
    }
}

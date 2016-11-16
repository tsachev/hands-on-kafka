package demo.consumer;

import java.awt.image.BufferedImage;

class Picture {
    final String title;
    final BufferedImage thumbnail;
    final String url;

    public Picture(String title, BufferedImage thumbnail, String url) {
        this.title = title;
        this.thumbnail = thumbnail;
        this.url = url;
    }
}

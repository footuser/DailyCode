/*
 * Copyright (c) 2012 Sohu. All Rights Reserved
 */
package model.zhihui;

import java.io.Serializable;

/**
 * 
 * @author zhihuiqiu
 *
 */
public class SucaiVideoSimple implements Serializable {

    /**
     * 
     */

    private static final long serialVersionUID = -6536584549987305041L;

    private Integer id;

    private Long vid;

    private Long pid;

    private String title;

    private String longTitle;

    private String shortTitle;

    private String url;

    private String horBigPic;

    private String horPic;

    private String verPic;

    private String bannerPic;

    private int weight;

    private Long aid;

    private Long category1;

    private Long category2;

    public Long getCategory1() {
        return category1;
    }

    public void setCategory1(Long category1) {
        this.category1 = category1;
    }

    public Long getCategory2() {
        return category2;
    }

    public void setCategory2(Long category2) {
        this.category2 = category2;
    }

    public Long getAid() {
        return aid;
    }

    public void setAid(Long aid) {
        this.aid = aid;
    }

    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    public Long getVid() {
        return vid;
    }

    public void setVid(Long vid) {
        this.vid = vid;
    }

    public Long getPid() {
        return pid;
    }

    public void setPid(Long pid) {
        this.pid = pid;
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public String getLongTitle() {
        return longTitle;
    }

    public void setLongTitle(String longTitle) {
        this.longTitle = longTitle;
    }

    public String getShortTitle() {
        return shortTitle;
    }

    public void setShortTitle(String shortTitle) {
        this.shortTitle = shortTitle;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public String getHorBigPic() {
        return horBigPic;
    }

    public void setHorBigPic(String horBigPic) {
        this.horBigPic = horBigPic;
    }

    public String getHorPic() {
        return horPic;
    }

    public void setHorPic(String horPic) {
        this.horPic = horPic;
    }

    public String getVerPic() {
        return verPic;
    }

    public void setVerPic(String verPic) {
        this.verPic = verPic;
    }

    public int getWeight() {
        return weight;
    }

    public void setWeight(int weight) {
        this.weight = weight;
    }

    public String getBannerPic() {
        return bannerPic;
    }

    public void setBannerPic(String bannerPic) {
        this.bannerPic = bannerPic;
    }

    @Override
    public String toString() {
        return "s[" + id + "]";
    }

}

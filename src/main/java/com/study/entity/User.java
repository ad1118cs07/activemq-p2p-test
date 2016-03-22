package com.study.entity;

import java.io.Serializable;
import java.util.Date;

public class User implements Serializable {
	
	private static final long serialVersionUID = 6383798674925888925L;
	private String name;
	private String gender;
	private Date createTime;
	
	public User() {
		
	}
	
	public User(String name, String gender, Date createTime) {
		super();
		this.name = name;
		this.gender = gender;
		this.createTime = createTime;
	}
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public String getGender() {
		return gender;
	}
	public void setGender(String gender) {
		this.gender = gender;
	}
	public Date getCreateTime() {
		return createTime;
	}
	public void setCreateTime(Date createTime) {
		this.createTime = createTime;
	}
	
	
}

package org.apache.carbondata.examples.sdk;

import java.io.Serializable;

public class LoginInfo implements Serializable {
  private boolean loggedIn = false;
  private String token;
  private String projectId;
  private String userName = "未登录";

  public String getToken() {
    return token;
  }

  public void setToken(String token) {
    this.token = token;
  }

  public String getProjectId() {
    return projectId;
  }

  public void setProjectId(String projectId) {
    this.projectId = projectId;
  }

  public String getUserName() {
    return userName;
  }

  public void setUserName(String userName) {
    this.userName = userName;
  }

  public boolean isLoggedIn() {
    return loggedIn;
  }

  public void setLoggedIn(boolean loggedIn) {
    this.loggedIn = loggedIn;
  }
}

package org.apache.carbondata.examples.sdk;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;

public class MatchScheduler {

  public static void main(String[] args) {
    generate(5);
    System.out.println("++++++++++++++++++++++++++++++++++++++++++++++++++++++++");
    generate(6);
  }

  public static class Team {

    private int number;

    public Team(int number) {
      this.number = number;
    }

    public int getNumber() {
      return number;
    }

    @Override public String toString() {
      return "Team{" + number + '}';
    }

    @Override public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      Team team = (Team) o;
      return number == team.number;
    }

    @Override public int hashCode() {
      return Objects.hash(number);
    }
  }

  public static class Week {

    private int weekNumber;

    public Week(int weekNumber) {
      this.weekNumber = weekNumber;
    }

    public int getWeekNumber() {
      return weekNumber;
    }

    @Override public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      Week week = (Week) o;
      return weekNumber == week.weekNumber;
    }

    @Override public int hashCode() {

      return Objects.hash(weekNumber);
    }

    @Override public String toString() {
      return "Week{" + weekNumber + '}';
    }
  }

  public static class Location {

    private Team location;

    public Location(Team location) {
      this.location = location;
    }

    public Team getLocation() {
      return location;
    }

    @Override public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      Location location1 = (Location) o;
      return Objects.equals(location, location1.location);
    }

    @Override public int hashCode() {
      return Objects.hash(location);
    }

    @Override public String toString() {
      return "Location{" + location + '}';
    }
  }

  public static class Fixture {

    private Location location;

    private Team left;

    private Team right;

    private Week week;

    public Fixture(Location location, Team left, Team right, Week week) {
      this.location = location;
      this.left = left;
      this.right = right;
      this.week = week;
    }

    public Location getLocation() {
      return location;
    }

    public Team getLeft() {
      return left;
    }

    public Team getRight() {
      return right;
    }

    public Week getWeek() {
      return week;
    }

    public void setWeek(Week week) {
      this.week = week;
    }

    @Override public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      Fixture fixture = (Fixture) o;
      return Objects.equals(location, fixture.location) && Objects.equals(left, fixture.left)
          && Objects.equals(right, fixture.right) && Objects.equals(week, fixture.week);
    }

    @Override public int hashCode() {

      return Objects.hash(location, left, right, week);
    }

    @Override public String toString() {
      return "Fixture{" + location + ", left=" + left + ", right=" + right + "," + week + '}';
    }
  }

  public static void generate(int actualNumber) {
    int number = actualNumber;
    boolean odd = true;
    if (actualNumber % 2 == 0) {
      number = actualNumber - 1;
      odd = false;
    }

    List<Fixture> fixtures = new ArrayList<>();

    for (int i = 1; i <= number; i++) {
      for (int j = 1; j <= number; j++) {
        Team l = new Team(i);
        Team r = new Team(j);
        if (i != j) {
          Fixture fixture = new Fixture(new Location(l), l, r, getWeek(i, j, number));
          fixtures.add(fixture);
        } else if (!odd) {
          r = new Team(number + 1);
          Fixture fixture = new Fixture(new Location(l), l, r, getWeek(i, j, number));
          fixtures.add(fixture);
        }
      }
    }

    for (Fixture fixture : fixtures) {
      System.out.println(fixture);
    }

    List<List<Fixture>> groups = getGroups(fixtures);
    // Fix the anomalies till exists
    while (findAndFixAnomalies(groups, number)) {
      groups = getGroups(fixtures);
    }
  }

  /**
   * Group them as per the week basis.
   */
  private static List<List<Fixture>> getGroups(List<Fixture> fixtures) {
    System.out.println("Week Groups");
    Collections.sort(fixtures, new Comparator<Fixture>() {
      @Override public int compare(Fixture o1, Fixture o2) {
        return Integer.compare(o1.week.getWeekNumber(), o2.week.getWeekNumber());
      }
    });
    List<List<Fixture>> groups = new ArrayList<>();
    Fixture old = null;
    List<Fixture> group = new ArrayList<>();
    for (Fixture fixture : fixtures) {
      if (old == null || !old.getWeek().equals(fixture.getWeek())) {
        group = new ArrayList<>();
        groups.add(group);
      }
      group.add(fixture);
      old = fixture;
    }

    for (List<Fixture> list : groups) {
      System.out.println("Week : " + list.get(0).getWeek().getWeekNumber());
      System.out.println("========================================");
      for (Fixture fixture : list) {
        System.out.println(fixture);
      }
    }
    return groups;
  }

  /**
   * Check if the same team plays on same week then change the number by incrementing it.
   */
  private static boolean findAndFixAnomalies(List<List<Fixture>> groups, int number) {
    boolean found = false;
    for (List<Fixture> group : groups) {
      for (Fixture fixture : group) {
        for (Fixture fixture1 : group) {
          if (fixture.left.equals(fixture1.right) && fixture.right.equals(fixture1.left)
              && fixture.week.equals(fixture1.week)) {
            System.out.println("Anomaly : " + fixture);
            found = true;
            int weekNumber = fixture.getWeek().getWeekNumber() + 1;
            if (weekNumber > number) {
              weekNumber = weekNumber - number;
            }
            fixture.setWeek(new Week(weekNumber));
            break;
          }
        }
      }
    }
    return found;
  }

  private static Week getWeek(int x, int y, int number) {
    if (x == 1) {
      return new Week(y);
    } else {
      if (x + y - 1 > number) {
        return new Week((x + y - 1) - number);
      } else {
        return new Week(x + y - 1);
      }
    }
  }

}

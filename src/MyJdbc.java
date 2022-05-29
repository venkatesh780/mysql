import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.*;
import java.util.LinkedHashMap;
import java.util.Map;

public class MyJdbc {
    public static void main(String[] args) throws Exception {
        String url = "jdbc:mysql://localhost:3306/iplprject";
        String uname = "root";
        String pass = "Venky@780";
        String query1 = "select season, count(season) as season_count from matches group by season;";
        String query2 = "select winner,count(winner) from matches group by winner;";
        String query3 = "select deliveries.batting_team, SUM(deliveries.extra_runs) as ConcesedRuns from matches inner join deliveries on matches.id= deliveries.match_id where season =2016 group by deliveries.batting_team;";
        String query4 = "select deliveries.bowler, SUM(deliveries.total_runs)/CAST(count(deliveries.bowler )/6 AS decimal(0)) as Economy from matches inner join deliveries on matches.id= deliveries.match_id where season =2015 group by deliveries.bowler Order by Economy;";
        String query5 = "select toss_winner, count(toss_winner) as toss_count from matches group by toss_winner;";



        findNumberOfMatchesPlayedPerYear(query1, url, uname, pass);
        findNumberOfMatchesWonPerTeamInAllSeasons(query2, url, uname, pass);
        findExtrarunsConcededPerTeamIn2016(query3, url, uname, pass);
        findMostEconomicalBowlerIn2015(query4, url, uname, pass);
        findNumberOfTimesTeamOwnTOss(query5, url, uname, pass);

    }

    private static void findNumberOfMatchesPlayedPerYear(String query1, String url, String uname, String pass) throws SQLException {

        Map<String, Integer> numberOfMatchesPlayedPerYear = new LinkedHashMap<>();

        Connection con = DriverManager.getConnection(url, uname, pass);
        Statement st = con.createStatement();
        ResultSet rs = st.executeQuery(query1);

        while (rs.next()) {
            if (numberOfMatchesPlayedPerYear.containsKey(rs.getString("season"))) {
                numberOfMatchesPlayedPerYear.put(rs.getString("season"),numberOfMatchesPlayedPerYear.get((rs.getString("season")+rs.getInt("season_count"))));
            } else {
                numberOfMatchesPlayedPerYear.put(rs.getString("season"), rs.getInt("season_count"));
            }
        }
        System.out.println("numberOfMatchesPlayedPerYear");
        System.out.println(numberOfMatchesPlayedPerYear);

    }
    private static void findNumberOfMatchesWonPerTeamInAllSeasons(String query2, String url, String uname, String pass) throws SQLException {

        Map<String, Integer> numberOfMatchesWonPerTeamInAllSeasons = new LinkedHashMap<>();
        Connection con = DriverManager.getConnection(url, uname, pass);
        Statement st = con.createStatement();
        ResultSet rs = st.executeQuery(query2);

        while (rs.next()) {
            if (numberOfMatchesWonPerTeamInAllSeasons.containsKey(rs.getString(1))) {
                numberOfMatchesWonPerTeamInAllSeasons.put(rs.getString(1), numberOfMatchesWonPerTeamInAllSeasons.get(rs.getString(1) + rs.getInt(2)));
            } else {
                numberOfMatchesWonPerTeamInAllSeasons.put(rs.getString(1), rs.getInt(2));
            }
        }
        System.out.println("numberOfMatchesWonPerTeamInAllSeasons");
        System.out.println(numberOfMatchesWonPerTeamInAllSeasons);
        st.close();
        con.close();
    }
    private static void findExtrarunsConcededPerTeamIn2016(String query3, String url, String uname, String pass) throws SQLException {
        Map<String, Integer> numberOfconcededRunsPerTeam = new LinkedHashMap<>();
        Connection con = DriverManager.getConnection(url, uname, pass);
        Statement st = con.createStatement();
        ResultSet rs = st.executeQuery(query3);

        while (rs.next()) {
            if (numberOfconcededRunsPerTeam.containsKey(rs.getString(1))) {
                numberOfconcededRunsPerTeam.put(rs.getString(1), numberOfconcededRunsPerTeam.get(rs.getString(1) + rs.getInt(2)));
            } else {
                numberOfconcededRunsPerTeam.put(rs.getString(1), rs.getInt(2));
            }
        }
        System.out.println("numberOfConcededRunsPerTeam");
        System.out.println(numberOfconcededRunsPerTeam);
        st.close();
        con.close();

    }
    private static void findMostEconomicalBowlerIn2015(String query4, String url, String uname, String pass) throws SQLException {
        Map<String, Integer> mostEconomicalBowlerIn2015 = new LinkedHashMap<>();
        Connection con = DriverManager.getConnection(url, uname, pass);
        Statement st = con.createStatement();
        ResultSet rs = st.executeQuery(query4);

        while (rs.next()) {
            if (mostEconomicalBowlerIn2015.containsKey(rs.getString(1))) {
                mostEconomicalBowlerIn2015.put(rs.getString(1), mostEconomicalBowlerIn2015.get(rs.getString(1) + rs.getInt(2)));
            } else {
                mostEconomicalBowlerIn2015.put(rs.getString(1), rs.getInt(2));
            }
        }
        System.out.println("Most economica bowlers in 2015");
        System.out.println(mostEconomicalBowlerIn2015);
        st.close();
        con.close();
    }
    private static void findNumberOfTimesTeamOwnTOss(String query5, String url, String uname, String pass) throws SQLException {
        Map<String, Integer> numberTimesTossOwnByTeams = new LinkedHashMap<>();

        Connection con = DriverManager.getConnection(url, uname, pass);
        Statement st = con.createStatement();
        ResultSet rs = st.executeQuery(query5);

        while (rs.next()) {
            if (numberTimesTossOwnByTeams.containsKey(rs.getString(1))) {
                numberTimesTossOwnByTeams.put(rs.getString(1),numberTimesTossOwnByTeams.get((rs.getString(1)+rs.getInt(2))));
            } else {
                numberTimesTossOwnByTeams.put(rs.getString(1), rs.getInt(2));
            }
        }
        System.out.println("number of times toss own");
        System.out.println(numberTimesTossOwnByTeams);
    }








}

import org.sqlite.SQLiteConfig;

import java.sql.*;
import java.util.Date;
import java.util.Scanner;

public class SQLiteJDBCExample {
    public static final String JDBC_URL = "jdbc:sqlite:/home/ehtesham/Practice";
    public static final String DB_DRIVER_CLASS = "org.sqlite.JDBC";
    private Connection connection = null;

    static {
        try {
            Class.forName(DB_DRIVER_CLASS);

        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    private void openConnection() throws SQLException {
        if (connection == null || connection.isClosed()) {
            SQLiteConfig config = new SQLiteConfig();
            config.enforceForeignKeys(true);
            connection = DriverManager.getConnection(JDBC_URL, config.toProperties());
        }
    }

    private void closeConnection() throws SQLException {
        connection.close();
    }

    void createSchema() throws SQLException {
        final Statement statement = connection.createStatement();
        statement.executeUpdate("CREATE TABLE course " +
                "(course_id INTEGER PRIMARY KEY ," +
                "title TEXT, seat_available INTEGER);");
        statement.executeUpdate("CREATE TABLE IF NOT EXISTS student (student_id INTEGER, name TEXT, PRIMARY KEY(student_id))");
        statement.executeUpdate("CREATE TABLE IF NOT EXISTS take (course_id INTEGER, student_id INTEGER, enroll_date TEXT, PRIMARY KEY(student_id, course_id))");
    }

    void initSchema() throws SQLException {
        final String[] courses = new String[]{"1,CloudComputing,200",
                "2,ArtificialIntelligence,70", "3,Cryptography,80"};
        final String[] students = new String[]{"11,Ehtesham", "12,Ammar",
                "13,Ashraf", "14,Fahmi", "15,Ali", "16,Abdur", "17,Saquib",
                "18,Zeeshan", "19,Adnan", "20,Nashit"};
        final String[] takes = new String[]{
                "1,11,2017-08-01",
                "2,13,2017-09-01", "2,14,2017-08-15",
                "3,11,2017-09-01", "3,12,2017-08-15"};
        PreparedStatement preparedStatement = connection.prepareStatement("INSERT INTO student VALUES(?,?");
        for (String s : students) {
            final String[] cols = s.split(",");
            preparedStatement.setLong(1, Long.valueOf(cols[0]));
            preparedStatement.setString(2, cols[1]);
            preparedStatement.executeUpdate();
        }

        PreparedStatement prStatement = connection.prepareStatement("INSERT INTO take VALUES(?,?,?)");
        for(String t :takes){
            final String[] cols=t.split(",");
            prStatement.setLong(1,Long.valueOf(cols[1]));
            prStatement.setLong(2,Long.valueOf(cols[1]));
            preparedStatement.setString(3,cols[2]);
            prStatement.executeUpdate();
        }
        PreparedStatement crs= connection.prepareStatement("INSERT INTO course VALUES(?,?,?");
        for (String c : courses) {
            final String[] cols = c.split(",");
            crs.setLong(1, Long.valueOf(cols[0]));
            crs.setString(2, cols[1]);
            crs.setInt(3, Integer.valueOf(cols[2]));
            crs.executeUpdate();
        }
    }
    void enroll() throws SQLException {
        final Scanner scanner=new Scanner(System.in);
        System.out.println("Please Enter student id:");
        final long studentId=scanner.nextLong();
        System.out.println("Please enter course id:");
        final long courseId= scanner.nextLong();
        //Once validated, a row will be inserted to "take" table and number of available seats will be decremented in course.
        if(!validate(studentId,courseId))
            return;
        final PreparedStatement tkStmt = connection.prepareStatement("INSERT INTO take VALUES (?, ?, ?)");
        tkStmt.setLong(1, courseId);
        tkStmt.setLong(2, studentId);
        tkStmt.setString(3, String.format("%1$tY-%1$tm-%1$td %1$tT", new Date()));
        tkStmt.executeUpdate();

        final PreparedStatement crsStmt = connection.prepareStatement("UPDATE course SET seats_available = seats_available - 1 WHERE course_id = ?");
        crsStmt.setLong(1, courseId);
        crsStmt.executeUpdate();

        System.out.printf("Student %d successfully enrolled in course %d\n", studentId, courseId);
    }
    private boolean validate(long studentId,long courseId) throws SQLException{
        Statement statement = connection.createStatement();
        if(!studentExists(statement,studentId))
        {
            System.out.println(String.format("[ERROR] enroll :student %d not found.\", studentId"));
            return false;
        }
        if(!courseExists(statement,courseId)){
            System.out.println(String.format("[ERROR] enroll: course %d not found.\", courseId"));
            return false;
        }
        if(!isSeatsAvailable(statement,courseId)){
            System.err.println(String.format("[ERROR] enroll : course %d is full.", courseId));
            return false;
        }
        if (isCurrentlyEnrolled(statement, studentId, courseId)) {
            System.err.println(String.format("[ERROR] enroll : student %d already enrolled in course %d.", studentId, courseId));
            return false;
        }
        return true;
    }
    void paginate() {
        try {
            final PreparedStatement pstmt = connection.prepareStatement("SELECT * FROM student WHERE student_id > ? ORDER BY student_id ASC LIMIT ?");

            final Scanner scanner = new Scanner(System.in);

            long lastId = 0;
            System.out.print("Enter page size (an integer in [1,5]): ");
            int pageSize = scanner.nextInt();
            if (!validatePageSize(pageSize)) return;

            scanner.reset();

            int page = 1;
            while (true) {
                pstmt.setLong(1, lastId);
                pstmt.setInt(2, pageSize);
                final ResultSet rs = pstmt.executeQuery();

                boolean isEmptyPage = true;
                while (rs.next()) {
                    if (isEmptyPage) {
                        System.out.println("Page " + page++);
                        System.out.println("+---|--------+");
                        System.out.printf("|%-3s|%-8s|\n", "id", "name");
                        System.out.println("+---|--------+");
                    }

                    isEmptyPage = false;

                    final long stdId = rs.getLong("student_id");
                    final String stdName = rs.getString("name");
                    System.out.printf("|%-3d|%-8s|\n", stdId, stdName);
                    lastId = stdId;
                }

                if (isEmptyPage) break;

                System.out.println("+---|--------+\n");
            }

        } catch (SQLException e) {
            System.err.println("[ERROR] paginate : " + e.getMessage());
        }
    }
    private boolean validatePageSize(int pageSize) {
        if (pageSize > 5 || pageSize < 1) {
            System.err.println("[ERROR] paginate : page size must be in range [1,5]");
            return false;
        }

        return true;
    }

    private boolean isCurrentlyEnrolled(Statement statement, long studentId, long courseId) throws SQLException {
        final ResultSet rs=statement.executeQuery("SELECT * FROM take WHERE course_id="+ courseId+" AND student_id = " + studentId);
        final boolean enrolled=rs.next();
        rs.close();
        return enrolled;
    }
    private boolean isSeatsAvailable(Statement statement,long courseId) throws SQLException {
        final ResultSet rs=statement.executeQuery("SELECT * FROM course WHERE course_id="+ courseId+ " AND seats_available > 0");
        boolean available=rs.next();
        rs.close();
        return available;
    }
    private boolean courseExists(Statement stmt, long courseId) throws SQLException {
        final ResultSet rs = stmt.executeQuery("SELECT course_id FROM course WHERE course_id = " + courseId);
        final boolean exists = rs.next();
        rs.close();
        return exists;
    }
    private boolean studentExists(Statement stmt, long studentId) throws SQLException {
        final ResultSet rs = stmt.executeQuery("SELECT student_id FROM student WHERE student_id = " + studentId);
        final boolean exists = rs.next();
        rs.close();
        return exists;
    }

    public static void main(String[] args) throws SQLException {
        if (args == null || args.length == 0 || !args[0].toLowerCase().matches("(paginate|enroll)")) {
            System.err.println("The program requires an argument, which can be either 'paginate' or 'enroll'");
            System.exit(1);
        }

        SQLiteJDBCExample example = new SQLiteJDBCExample();
        example.openConnection();
        example.createSchema();
        example.initSchema();
        System.out.println("Initialization complete!!");

        if (args[0].equalsIgnoreCase("paginate")) {
            example.paginate();
        } else {
            example.enroll();
        }
        example.closeConnection();

    }
}

package hbaseia.twitbase.model;

public class User {

    public String user;
    public String name;
    public String email;
    public String password;

    @Override
    public String toString() {
        return String.format("<User: %s %s %s>", this.user, this.name, this.email);
    }
}

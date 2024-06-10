package topfive;

import com.fasterxml.jackson.databind.ObjectMapper;
import model.ContributorWithCount;
import org.apache.kafka.common.serialization.Serializer;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class TopFiveSerializer implements Serializer<TopFiveContributors> {

    public static final ObjectMapper mapper = new ObjectMapper();

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        // No configuration needed
    }

    @Override
    public byte[] serialize(String topic, TopFiveContributors topFiveContributors) {
        try (final ByteArrayOutputStream out = new ByteArrayOutputStream()) {
            List<ContributorWithCount> contributors = new ArrayList<>();
            for (ContributorWithCount c : topFiveContributors) {
                if (c == null) continue;
                contributors.add(c);
            }
            out.write(mapper.writeValueAsBytes(contributors));
            return out.toByteArray();
        } catch (IOException e) {
            throw new IllegalStateException("Error serializing JSON message", e);
        }
    }

    @Override
    public void close() {
    }

    public static void main(String[] args) {
        TopFiveSerializer t = new TopFiveSerializer();
        var contributors = Arrays.asList(
                new ContributorWithCount("repo2", "email2.gmail.com", 2L),
                new ContributorWithCount("repo1", "email.gmail.com", 3L),
                new ContributorWithCount("repo3", "email.gmail.com", 1L),
                new ContributorWithCount("repo3", "email.gmail.com", 5L),
                new ContributorWithCount("repo3", "email.gmail.com", 0L),
                new ContributorWithCount("repo3", "email.gmail.com", 1L),
                new ContributorWithCount("repo3", "email.gmail.com", 11L),
                new ContributorWithCount("repo3", "email.gmail.com", -1L)
        );
        TopFiveContributors topFive = new TopFiveContributors();

        for (var c : contributors) {
            topFive.add(c);
        }

        for (ContributorWithCount c : topFive) {
            System.out.println("first" + c);
        }

        for (ContributorWithCount c : topFive) {
            System.out.println("sec" + c);
        }
        System.out.println("--------------------");
        byte[] bytes = t.serialize("", topFive);
        TopFiveContributors topFiveDeserialized = new TopFiveDeserializer().deserialize("", bytes);
        for (ContributorWithCount c : topFiveDeserialized) {
            System.out.println("3" + c);
        }

        for (ContributorWithCount c : topFiveDeserialized) {
            System.out.println("4" + c);
        }
        System.out.println(new String(bytes));
    }
}

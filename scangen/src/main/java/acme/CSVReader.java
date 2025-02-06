package acme;

import java.io.IOException;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import java.util.stream.StreamSupport;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

public class CSVReader<T> {

    public List<T> read(String filePath, Function<CSVRecord, T> mapper) {
        List<T> items = new ArrayList<>();

        try (Reader reader = Files.newBufferedReader(Paths.get(filePath));
                CSVParser csvParser = new CSVParser(reader,
                        CSVFormat.DEFAULT.withFirstRecordAsHeader().withIgnoreHeaderCase().withTrim())) {

            for (CSVRecord csvRecord : csvParser) {
                // Skip if the record is null or all fields are empty
                if (csvRecord == null
                        || StreamSupport.stream(csvRecord.spliterator(), false).anyMatch(String::isEmpty)) {
                    continue;
                }
                T item = mapper.apply(csvRecord);
                items.add(item);
            }

        } catch (IOException e) {
            e.printStackTrace();
        }

        return items;
    }
}

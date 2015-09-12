package net.franckbenault.asertjdb.s00firsttest;

import static com.ninja_squad.dbsetup.Operations.deleteAllFrom;
import static com.ninja_squad.dbsetup.Operations.insertInto;
import static com.ninja_squad.dbsetup.Operations.sequenceOf;

import java.sql.Connection;
import java.sql.Date;
import java.sql.SQLException;
import java.sql.Statement;

import org.assertj.db.type.DateValue;
import org.assertj.db.type.Table;
import org.h2.jdbcx.JdbcConnectionPool;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.ninja_squad.dbsetup.DbSetup;
import com.ninja_squad.dbsetup.destination.DataSourceDestination;
import com.ninja_squad.dbsetup.operation.Operation;

import static org.assertj.db.api.Assertions.assertThat;

public class TestSimpleWithH2 {

	private static JdbcConnectionPool dataSource;

	  /**
	   * Création de la source de données (globale à la classe).
	   * Utilisation de JDBC pour cette création.
	   * @throws SQLException
	   */
	  @BeforeClass
	  public static void setUpGlobal() throws SQLException {
	    if (dataSource == null) {
	      dataSource = JdbcConnectionPool.create("jdbc:h2:mem:test;DB_CLOSE_DELAY=-1", "user", "password");
	      try (Connection connection = dataSource.getConnection()) {
	        try (Statement statement = connection.createStatement()) {
	          statement.executeUpdate("create table membres("
	              + "id number primary key, "
	              + "nom varchar not null, "
	              + "prenom varchar not null, "
	              + "surnom varchar, "
	              + "date_naissance date, "
	              + "taille decimal);");
	        }
	      }
	    }
	  }

	  /**
	   * Chargement des données en base (avant chaque test).
	   * Utilisation de DBSetup (effacement de toutes les données puis insertion).
	   */
	  @Before
	  public void setUp() {
	    Operation OperationInsert = insertInto("membres")
	        .columns("id", "nom", "prenom", "surnom", "date_naissance", "taille")
	        .values(1, "Hewson", "Paul David", "Bono", Date.valueOf("1960-05-10"), 1.75)
	        .values(2, "Evans", "David Howell", "The Edge", Date.valueOf("1961-08-08"), 1.77)
	        .values(3, "Clayton", "Adam", null, Date.valueOf("1960-03-13"), 1.78)
	        .values(4, "Mullen", "Larry", null, Date.valueOf("1961-10-31"), 1.7)
	        .build();

	    DbSetup dbSetup = new DbSetup(new DataSourceDestination(dataSource), 
	        sequenceOf(deleteAllFrom("membres"), OperationInsert));

	    dbSetup.launch();
	  }

	  /**
	   * Test simple utilisant AssertJ-DB.
	   */
	  @Test
	  public void testSimple() {
	    // Objet Table sur la table "membres" de la DataSource
	    Table table = new Table (dataSource, "membres");

	    // Vérifie que la table contient 4 enregistrements
	    assertThat(table).hasNumberOfRows(4);
	    // Vérifie que la valeur de la colonne "nom" de l'enregistrement à l'index 2
	    // est égal à la chaîne de caractères "Clayton"
	    assertThat(table).row(2).value("nom").isEqualTo("Clayton");
	    // Vérifie que la valeur à l'index 3 de la colonne "surnom" est null
	    assertThat(table).column("surnom").value(3).isNull();
	    // Vérifie que la colonne à l'index 4 a pour nom "date_naissance"
	    // puis que la valeur à l'index 1 de cette colonne
	    // est égal à la date 08/08/1960
	    assertThat(table).column(4).hasColumnName("date_naissance")
	                     .value(1).isEqualTo(DateValue.of(1961, 8, 8));
	  }


}

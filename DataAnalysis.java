package com.mycompany.mavenproject1;
import java.io.File;
import java.io.IOException;
import java.util.*;
import com.github.javafaker.Faker;
import org.neo4j.io.fs.FileUtils;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.graphdb.index.ReadableIndex;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.traversal.Evaluators;
import org.neo4j.graphdb.traversal.TraversalDescription;
import org.neo4j.graphdb.traversal.Uniqueness;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.Direction;
import static java.lang.System.out;

//data model:
//nodes:
//(manager1:MANAGER {name:'bob', age:40)
//(designer1:DESIGNER {name:'ana', age:30)
//(programmer1:PROGRAMMER {name:'zoey', age:30)
//(artist1:ARTIST {name:'sven', age:30)
//(product1:PRODUCT {name:'Coca Cola', price:'2.2', daily sales:5000, index:0)

//relationship:
//managers manage designers
//managers manage programmers
//managers manage artists
//designers design products
//programmers code products
//artist draw products

//target:
//1. find out which product has the highest sals
//2. find out who is involved in the development of product that has the lowest sals.
//3. find out who design the product of lowest sals.
//4. find out which programmer is involed in the top10 best-selling product for the most times.
//5. find out which manger have the highest sales of product, 
//e.g. a manager may manages 2 artist, 3 programmer, 4 designer, these people are invovled in different type of product, say, 4
//so the manager's sales of product is the sum of 4 products' sales.

public class DataAnalysis 
{
    private static final int mangerNum = 5;
    private static final int designerNum = 10;
    private static final int programmerNum = 15;
    private static final int artistNum = 12;
    private static final int productNum = 50;
    private static final File databaseDirectory = new File( "D:/ZC/DB/MyDB" );
    private TraversalDescription friendsTraversal;
    private GraphDatabaseService graphDb;
    private Node[] managerArray = new Node[mangerNum];
    private Node[] designerArray = new Node[designerNum];
    private Node[] programmerArray = new Node[programmerNum];
    private Node[] artistArray = new Node[artistNum];
    private Node[] productArray = new Node[productNum];
    private List<Node> nodeList = new ArrayList<>();
    private List<Relationship> relationshipList = new ArrayList<>();
    private Node highestSalesProduct;
    private Node lowestSalesProduct;
    private static enum RelTypes implements RelationshipType
    {
        MANAGE,
        DESIGN,
        CODE,
        DRAW;
    }
    
    public DataAnalysis( GraphDatabaseService db )
    {
        this.graphDb = db;
        friendsTraversal = db.traversalDescription()
                .depthFirst()
                .relationships( RelTypes.MANAGE )
                .uniqueness( Uniqueness.RELATIONSHIP_GLOBAL );
        registerShutdownHook( graphDb );
    }
    
    public static void main( String[] args )  throws IOException
    {  
        FileUtils.deleteRecursively( databaseDirectory );
        GraphDatabaseService database = new GraphDatabaseFactory().newEmbeddedDatabase( databaseDirectory );
        DataAnalysis example = new DataAnalysis( database );
        example.createData();
        example.run();
    }
      
    private void createData()
    {
        Faker faker = new Faker();
        try ( Transaction tx = graphDb.beginTx())
        {
            //create MANAGER
            for (int i = 0; i < mangerNum; ++i)
            {
                Node manager = graphDb.createNode( Label.label( "MANAGER" ));
                manager.setProperty( "name", faker.name().fullName());
                manager.setProperty( "age", faker.number().numberBetween(30, 60));
                managerArray[i] = manager;
                nodeList.add(manager);
            }
            
            //create DESIGNER  
            for (int i = 0; i < designerNum; ++i)
            {
                Node designer = graphDb.createNode( Label.label( "DESIGNER" ));
                designer.setProperty( "name", faker.name().fullName());
                designer.setProperty( "age", faker.number().numberBetween(22, 50));
                designerArray[i] = designer;
                nodeList.add(designer);
            }
            
             //create PROGRAMMER           
            for (int i = 0; i < programmerNum; ++i)
            {
                Node programmer = graphDb.createNode( Label.label( "PROGRAMMER" ));
                programmer.setProperty( "name", faker.name().fullName());
                programmer.setProperty( "age", faker.number().numberBetween(22, 50));
                programmerArray[i] = programmer;
                nodeList.add(programmer);
            }
            
            //create ARTIST
            for (int i = 0; i < artistNum; ++i)
            {
                Node artist = graphDb.createNode( Label.label( "ARTIST" ));
                artist.setProperty( "name", faker.name().fullName());
                artist.setProperty( "age", faker.number().numberBetween(22, 50));
                artistArray[i] = artist;
                nodeList.add(artist);
            }
            
            //create PRODUCT           
            for (int i = 0; i < productNum; ++i)
            {
                Node product = graphDb.createNode( Label.label( "PRODUCT" ));
                product.setProperty( "name", faker.commerce().productName());
                product.setProperty( "price", faker.commerce().price());
                product.setProperty( "daily_sales", faker.number().numberBetween(0, 99999));
                 product.setProperty( "index", i);
                productArray[i] = product;
                nodeList.add(product);  
            }
            
            //manager manage designer/artist/programmer
            for (int i = 0; i < mangerNum; ++i)
            {
                //randomly pick some designers under management
                int desNum = faker.number().numberBetween(1, 4);
                for (int j = 0; j < desNum; ++j)
                {
                    Node desNode = designerArray[faker.number().numberBetween(0, designerNum - 1)];
                    Relationship manager_M_des = managerArray[i].createRelationshipTo( desNode, RelTypes.MANAGE );
                    relationshipList.add(manager_M_des);
                }
                
                //randomly pick some programmers under management
                int progNum = faker.number().numberBetween(1, 4);
                for (int j = 0; j < progNum; ++j)
                {
                    Node progNode = programmerArray[faker.number().numberBetween(0, programmerNum - 1)];
                    Relationship manager_M_prog = managerArray[i].createRelationshipTo( progNode, RelTypes.MANAGE );
                    relationshipList.add(manager_M_prog);
                }
                 
                //randomly pick some artists under management
                 int artNum = faker.number().numberBetween(1, 4);
                for (int j = 0; j < artNum; ++j)
                {
                    Node artNode = artistArray[faker.number().numberBetween(0, artistNum - 1)];
                    Relationship manager_M_art = managerArray[i].createRelationshipTo( artNode, RelTypes.MANAGE );
                    relationshipList.add(manager_M_art);
                }      
            }
 
            //product is designed/drew/coded by designer/artist/programmer
            for (int i = 0; i < productNum; ++i)
            {
                //randomly pick some designers under design
                int desNum = faker.number().numberBetween(1, 4);
                for (int j = 0; j < desNum; ++j)
                {
                    Node desNode = designerArray[faker.number().numberBetween(0, designerNum - 1)];
                    Relationship product_Des_des = desNode.createRelationshipTo( productArray[i], RelTypes.DESIGN );
                    relationshipList.add(product_Des_des);
                }
                
                //randomly pick some programmers under code
                int progNum = faker.number().numberBetween(1, 4);
                for (int j = 0; j < progNum; ++j)
                {
                    Node progNode = programmerArray[faker.number().numberBetween(0, programmerNum - 1)];
                    Relationship product_C_prog = progNode.createRelationshipTo( productArray[i], RelTypes.CODE );
                    relationshipList.add(product_C_prog);
                }
                 
                //randomly pick some artists under draw
                 int artNum = faker.number().numberBetween(1, 4);
                for (int j = 0; j < artNum; ++j)
                {
                    Node artNode = artistArray[faker.number().numberBetween(0, artistNum - 1)];
                    Relationship product_Draw_art = artNode.createRelationshipTo( productArray[i], RelTypes.DRAW );
                    relationshipList.add(product_Draw_art);
                }      
            }
            tx.success();
        }   
    }

   //1. find out which product has the highest sals
   private void Query1()
   {
        boolean flag = true;
        try ( Transaction ignored = graphDb.beginTx();  
              Result result = graphDb.execute( "MATCH (n:PRODUCT) RETURN n, n.name, n.daily_sales ORDER BY n.daily_sales DESC LIMIT 1" ) )
        {
            while ( result.hasNext() )
            {
                Map<String, Object> row = result.next();
                for ( String key : result.columns() )
                {
                    if (flag)
                    {
                        highestSalesProduct = (Node)row.get( key );
                        flag = false;
                    }
                    System.out.printf( "%s = %s%n", key, row.get( key ) );
                }
            }
        }
   }
   
   //2. find out who is involved in the development of product that has the highest sals.
   private void Query2()
   {
        try (Transaction tx = graphDb.beginTx())
        {
             String output = "";  
            for ( Path position : graphDb.traversalDescription()
                .depthFirst()
                .relationships( RelTypes.MANAGE, Direction.INCOMING )
                .relationships( RelTypes.DESIGN, Direction.INCOMING)
                .relationships( RelTypes.DRAW, Direction.INCOMING)
                .relationships( RelTypes.CODE, Direction.INCOMING)
                .uniqueness( Uniqueness.RELATIONSHIP_GLOBAL )
                .evaluator( Evaluators.toDepth( 5 ) )
                .traverse( highestSalesProduct ) )
            {
                output += position + "\n";
            }
            System.out.println( output );
        }
   }
   
   //3. find out who design the product of lowest sals.
   private void Query3()
   {
       boolean flag = true;
        try ( Transaction ignored = graphDb.beginTx();  
              Result result = graphDb.execute( "MATCH (n:PRODUCT) RETURN n, n.name, n.daily_sales ORDER BY n.daily_sales LIMIT 1" ) )
        {
            while ( result.hasNext() )
            {
                Map<String, Object> row = result.next();
                for ( String key : result.columns() )
                {
                    if (flag)
                    {
                        lowestSalesProduct = (Node)row.get( key );
                        flag = false;
                    }
                    System.out.printf( "%s = %s%n", key, row.get( key ) );
                }
            }
            
            String output = "";  
            for ( Node currentNode : graphDb.traversalDescription()
                .depthFirst()
                .relationships( RelTypes.DESIGN, Direction.INCOMING)
                .uniqueness( Uniqueness.RELATIONSHIP_GLOBAL )
                .evaluator( Evaluators.toDepth( 5 ) )
                .traverse( lowestSalesProduct )
                .nodes())
            {
                 output += currentNode.getProperty( "name" ) + "\n";
            }
            System.out.println(output);
        }
   }
   
   //4. find out which programmer is involed in the top10 best-selling product for the most times.
   private void Query4()
   {
       
   }

    //5. find out which manger have the highest sales of product, 
    //e.g. a manager may manages 2 artist, 3 programmer, 4 designer, these people are invovled in different type of product, say, 4
    //so the manager's sales of product is the sum of 4 products' sales.
    private void Query5()
   {
       
   }

    private void run()
    {
        Query1();
        Query2();
        Query3();
        Query4();
        Query5();
    }
    
    private static void registerShutdownHook( final GraphDatabaseService graphDb ) 
    {
        // Registers a shutdown hook for the Neo4j instance so that it
        // shuts down nicely when the VM exits (even if you "Ctrl-C" the
        // running application).
        Runtime.getRuntime().addShutdownHook( new Thread() 
        {
            @Override
            public void run()
            {
                graphDb.shutdown();
            }
        } );
    }
    
//    public String ManageDessignDrawCodeTraverser( Node node )
//    {
//        String output = "";  
//        for ( Path position : graphDb.traversalDescription()
//                .depthFirst()
//                .relationships( RelTypes.MANAGE )
//                .relationships( RelTypes.DESIGN)
//                .relationships( RelTypes.DRAW)
//                .relationships( RelTypes.CODE)
//                .uniqueness( Uniqueness.RELATIONSHIP_GLOBAL )
//                .evaluator( Evaluators.toDepth( 5 ) )
//                .traverse( node ) )
//        {
//            output += position + "\n";
//        }    
//        return output;
//    }
    
}



# spanner-hibernate
Hibernate Dialect for Google Cloud Spanner

Use this Dialect in combination with my Google Cloud Spanner JDBC Driver (spanner-jdbc, https://github.com/olavloite/spanner-jdbc) if you want to develop applications using JPA/Hibernate and Google Cloud Spanner.

Releases are available on Maven Central. Current release is version 0.7.

<div class="highlight highlight-text-xml"><pre>
	&lt;<span class="pl-ent">dependency</span>&gt;
    		&lt;<span class="pl-ent">groupId</span>&gt;nl.topicus&lt;/<span class="pl-ent">groupId</span>&gt;
    		&lt;<span class="pl-ent">artifactId</span>&gt;spanner-hibernate&lt;/<span class="pl-ent">artifactId</span>&gt;
    		&lt;<span class="pl-ent">version</span>&gt;0.7&lt;/<span class="pl-ent">version</span>&gt;
	&lt;/<span class="pl-ent">dependency</span>&gt;
</pre></div>

## Generating schema
The dialect supports the automatic generation of the required schema from the metamodel. Executing DDL statements on Google Cloud Spanner can be relatively slow. In order to speed this up, the JDBC driver of Google Cloud Spanner supports automatic batching of DDL statements. This dialect can utilize this functionality of the JDBC driver by setting a custom SchemaManagementTool like this:

`hibernate.schema_management_tool=nl.topicus.hibernate.dialect.CloudSpannerSchemaManagementTool`

Have a look at this sample project on how to use this setting: https://github.com/olavloite/spring-boot/tree/master/spring-boot-samples/spring-boot-sample-data-rest

# DEPRECATED
This project is no longer actively maintained.

It is recommended to use the officially supported Hibernate dialect that can be found at https://github.com/GoogleCloudPlatform/google-cloud-spanner-hibernate in combination with the offically supported open source JDBC driver found at https://github.com/googleapis/java-spanner-jdbc.

## spanner-hibernate
Hibernate Dialect for Google Cloud Spanner

This dialect can be used in combination with the community open source JDBC Driver for Google Cloud Spanner (spanner-jdbc, https://github.com/olavloite/spanner-jdbc).

Releases are available on Maven Central. Current release is version 0.8.

<div class="highlight highlight-text-xml"><pre>
	&lt;<span class="pl-ent">dependency</span>&gt;
    		&lt;<span class="pl-ent">groupId</span>&gt;nl.topicus&lt;/<span class="pl-ent">groupId</span>&gt;
    		&lt;<span class="pl-ent">artifactId</span>&gt;spanner-hibernate&lt;/<span class="pl-ent">artifactId</span>&gt;
    		&lt;<span class="pl-ent">version</span>&gt;0.8&lt;/<span class="pl-ent">version</span>&gt;
	&lt;/<span class="pl-ent">dependency</span>&gt;
</pre></div>

## Generating schema
The dialect supports the automatic generation of the required schema from the metamodel. Executing DDL statements on Google Cloud Spanner can be relatively slow. In order to speed this up, the JDBC driver of Google Cloud Spanner supports automatic batching of DDL statements. This dialect can utilize this functionality of the JDBC driver by setting a custom SchemaManagementTool like this:

`hibernate.schema_management_tool=nl.topicus.hibernate.dialect.CloudSpannerSchemaManagementTool`

Have a look at this sample project on how to use this setting: https://github.com/olavloite/spring-boot/tree/master/spring-boot-samples/spring-boot-sample-data-rest

{#
	For more information on how this XML trick works with splitting strings, see https://www.mssqltips.com/sqlservertip/1771/splitting-delimited-strings-using-xml-in-sql-server/
	On Azure SQL and SQL Server 2019, we can use the string_split function instead of the XML trick.
	But since we don't know which version of SQL Server the user is using, we'll stick with the XML trick in this adapter.
	However, since the XML data type is not supported in Synapse, it has to be overriden in that adapter.
#}

{% macro sqlserver__split_part(string_text, delimiter_text, part_number) %}

    LTRIM(CAST(('<X>'+REPLACE({{ string_text }},{{ delimiter_text }} ,'</X><X>')+'</X>') AS XML).value('(/X)[{{ part_number }}]', 'VARCHAR(128)'))

{% endmacro %}

{#
	For more information on how this XML trick works with splitting strings, see https://www.mssqltips.com/sqlservertip/1771/splitting-delimited-strings-using-xml-in-sql-server/
#}

{% macro sqlserver__split_part(string_text, delimiter_text, part_number) %}

    LTRIM(CAST(('<X>'+REPLACE({{ string_text }},{{ delimiter_text }} ,'</X><X>')+'</X>') AS XML).value('(/X)[{{ part_number }}]', 'VARCHAR(128)'))

{% endmacro %}

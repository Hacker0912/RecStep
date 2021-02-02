java -jar antlr-4.8-complete.jar  -no-listener -no-visitor -Dlanguage=Python3 Datalog.g4
mv DatalogLexer.py datalog_lexer.py
mv DatalogParser.py datalog_parser.py

grammar Datalog;

@header{
}

@parser::members {

class AtomArg():
    def __init__(self, arg_name, arg_type):
        self.name = arg_name
        self.type = arg_type
}

datalog_edb_declare returns[r]
    : {edb_list = []}
      TOKEN_EDB TOKEN_COLON
      schema1 = datalog_relation_schema   {edb_list.append($schema1.r)}
      (schema2 = datalog_relation_schema {edb_list.append($schema2.r)})*
      {$r = edb_list}
    ;

datalog_idb_declare returns [r]
    : {idb_list = []}
      TOKEN_IDB TOKEN_COLON
      schema1 = datalog_relation_schema   {idb_list.append($schema1.r)}
      (schema2 = datalog_relation_schema {idb_list.append($schema2.r)})*
      {$r = idb_list}
    ;

datalog_relation_schema returns [r]
    : {schema = {'name': '', 'attributes': []}}
      relation_name = TOKEN_ID                    {schema['name'] = $relation_name.text}
      TOKEN_LEFT_PAREN
      t1 = TOKEN_ID dt1 = data_type                {schema['attributes'].append(self.AtomArg($t1.text, $dt1.r))}
      (TOKEN_COMMA t2 = TOKEN_ID dt2 = data_type   {schema['attributes'].append(self.AtomArg($t2.text, $dt2.r))})*
      TOKEN_RIGHT_PAREN
      {$r = schema}
    ;

datalog_rule_declare returns [r]
    : TOKEN_RULE TOKEN_COLON dp = datalog_program {$r = $dp.r} EOF
    ;

datalog_program returns [r]
	: {rule_list = []}
	  r1 = datalog_rule  {rule_list.append($r1.r)} 
	  (r2 = datalog_rule {rule_list.append($r2.r)})*
	  {$r = rule_list}
	;

/** Rule that has no body must be the rule specifying "facts insertion" **/
datalog_rule returns [r]
    : {rule_dic = {}}
	    h = head {rule_dic['head'] = $h.r}
	    TOKEN_BODY_HEAD_SEP {rule_dic['body'] = None}
	    (b = body {rule_dic['body'] = $b.r})?
	    TOKEN_DOT
	  {$r = rule_dic}
    ;

head returns [r]
	: a = atom
	  {$r = $a.r}
	;

body returns [r] 
    : {body_dic = {'atoms':[], 'compares': [], 'assigns':[], 'negations':[]}}
	  ((b1 = atom 			{body_dic['atoms'].append($b1.r)} | 
	   	b2 = compare_expr 	{body_dic['compares'].append($b2.r)} | 
	  	b3 = assign 		{body_dic['assigns'].append($b3.r)} | 
	    b4 = negation 		{body_dic['negations'].append($b4.r)}) 
		TOKEN_COMMA)*
	  ((b5 = atom 			{body_dic['atoms'].append($b5.r)} |
		b6 = compare_expr 	{body_dic['compares'].append($b6.r)} |
		b7 = assign		    {body_dic['assigns'].append($b7.r)} |
		b8 = negation 		{body_dic['negations'].append($b8.r)}))
	  {$r = body_dic}
	;


negation returns [r]
	: TOKEN_NOT a = atom
	  {$r = $a.r}
	;

atom returns [r]
	: {atom_dic = {'name': None, 'arg_list':[]}}
	  a1 = TOKEN_ID 		  {atom_dic['name'] = $a1.text}  
	  TOKEN_LEFT_PAREN 
	  (a2 = TOKEN_ID 		  {atom_dic['arg_list'].append(self.AtomArg($a2.text, 'variable'))} |
	   a3 = aggregation_expr  {atom_dic['arg_list'].append(self.AtomArg($a3.r, 'aggregation'))} |
	   a4 = TOKEN_ANY 		  {atom_dic['arg_list'].append(self.AtomArg($a4.text, 'any'))} |
	   a5 = constant 		  {atom_dic['arg_list'].append(self.AtomArg($a5.text, 'constant'))} |
	   a6 = math_expr         {atom_dic['arg_list'].append(self.AtomArg($a6.r, 'math_expr'))})
	  (TOKEN_COMMA 
	   (a7 = TOKEN_ID 		  {atom_dic['arg_list'].append(self.AtomArg($a7.text, 'variable'))} |
		a8 = aggregation_expr {atom_dic['arg_list'].append(self.AtomArg($a8.r, 'aggregation'))}|
		a9 = TOKEN_ANY		  {atom_dic['arg_list'].append(self.AtomArg($a9.text, 'any'))} |
		a10 = constant 		  {atom_dic['arg_list'].append(self.AtomArg($a10.text, 'constant'))} |
		a11 = math_expr       {atom_dic['arg_list'].append(self.AtomArg($a11.r, 'math_expr'))}))*
	  TOKEN_RIGHT_PAREN
	  {$r = atom_dic}
	;

assign returns [r] 
	: {assign_dic = {}}
	  a1 = TOKEN_ID 	  {assign_dic['lhs'] = $a1.text} 
	  TOKEN_EQUALS
	  a2 = math_expr 	  {assign_dic['rhs'] = $a2.r}
	  {$r = assign_dic}
	;

math_expr returns [r]
	: {math_dic = {}}
	  m1 = TOKEN_ID		  {math_dic['lhs'] = $m1.text} 
	  m2 = math_op		  {math_dic['op'] = $m2.r}
      m3 = TOKEN_ID 	  {math_dic['rhs'] = $m3.text}
	  {$r = math_dic}
	;

compare_expr returns [r]
	: {compare_dic = {}}
      (c1 = TOKEN_ID      {compare_dic['lhs'] = [$c1.text, 'var']} |
       c2 = TOKEN_INTEGER  {compare_dic['lhs'] = [$c2.text, 'num']})
       op = compare_op	  {compare_dic['op'] = $op.r}
      (c4 = TOKEN_ID      {compare_dic['rhs'] = [$c4.text, 'var']} |
       c5 = TOKEN_INTEGER  {compare_dic['rhs'] = [$c5.text, 'num']})
	  {$r = compare_dic}
	; 

aggregation_expr returns [r]
	: {agg_dic = {'agg_op': None, 'agg_arg': None}}
	  a1 = aggregation_op {agg_dic['agg_op'] = $a1.r}
	  TOKEN_LEFT_PAREN
	  (a2 = TOKEN_ID 	  {agg_dic['agg_arg'] = {'type': 'attribute', 'content': $a2.text}} |
	   a3 = math_expr     {agg_dic['agg_arg'] = {'type': 'math_expr', 'content': $a3.r}})
	  TOKEN_RIGHT_PAREN 
	  {$r = agg_dic}
	;

compare_op returns [r]
	: op1 = TOKEN_NOT_EQUALS          {$r = $op1.text}         
    | op2 = TOKEN_EQUALS			  {$r = $op2.text}
	| op3 = TOKEN_GREATER_THAN		  {$r = $op3.text}
	| op4 = TOKEN_GREATER_EQUAL_THAN  {$r = $op4.text}
    | op5 = TOKEN_LESS_THAN			  {$r = $op5.text}
    | op6 = TOKEN_LESS_EQUAL_THAN	  {$r = $op6.text}
	;

aggregation_op returns [r]
	: op1 = TOKEN_MIN   {$r = $op1.text}
	| op2 = TOKEN_MAX   {$r = $op2.text}
	| op3 = TOKEN_SUM	{$r = $op3.text}
	| op4 = TOKEN_COUNT {$r = $op4.text}
	;

math_op returns [r] 
	: op1 = TOKEN_PLUS  {$r = $op1.text}
	| op2 = TOKEN_MINUS {$r = $op2.text}
	| op3 = TOKEN_MULT  {$r = $op3.text}
	| op4 = TOKEN_DIV	{$r = $op4.text}
	;	

constant returns [r]
	: c1 = TOKEN_INTEGER {$r = $c1.text}
	| c2 = TOKEN_STRING {$r = $c2.text}
	;

data_type returns [r]
    : dt1 = TOKEN_INT {$r = $dt1.text}
    | dt2 = TOKEN_FLOAT {$r = $dt2.text}
    | dt3 = TOKEN_DOUBLE {$r = $dt3.text}
    | dt4 = TOKEN_VARCHAR {$r = $dt4.text}
    | dt5 = TOKEN_CHAR {$r = $dt5.text}
    | dt6 = TOKEN_DATE {$r = $dt6.text}
    | dt7 = TOKEN_DATETIME {$r = $dt7.text}
    ;

/** Declaration **/
TOKEN_EDB: 'EDB_DECL';
TOKEN_IDB: 'IDB_DECL';
TOKEN_RULE: 'RULE_DECL';

/** Constants **/
TOKEN_INTEGER: [-+]?[0-9]+;
TOKEN_STRING: '\''([A-Za-z] | [0-9])+'\'';

/** Data Types **/
TOKEN_INT: ('i'|'I')('n'|'N')('t'|'T');
TOKEN_FLOAT: ('f'|'F')('l'|'L')('o'|'O')('a'|'A')('t'|'T');
TOKEN_DOUBLE: ('d'|'D')('o'|'O')('u'|'U')('b'|'B')('l'|'L')('e'|'E');
TOKEN_VARCHAR: ('v'|'V')('a'|'A')('r'|'R')('c'|'C')('h'|'H')('a'|'A')('r'|'R');
TOKEN_CHAR: ('c'|'C')('h'|'H')('a'|'A')('r'|'R');
TOKEN_DATE: ('d'|'D')('a'|'A')('t'|'T')('e'|'E');
TOKEN_DATETIME: ('d'|'D')('a'|'A')('t'|'T')('e'|'E')('t'|'T')('i'|'I')('m'|'M')('e'|'E');

/** Aggregation Operators **/
TOKEN_MIN: 'MIN';
TOKEN_MAX: 'MAX';
TOKEN_SUM: 'SUM';
TOKEN_COUNT: 'COUNT';

/**  Datalog Rules **/
TOKEN_ID: [A-Za-z]([A-Za-z]|[0-9]|'_')*;
TOKEN_BODY_HEAD_SEP: ':-';
TOKEN_ANY: '_';
TOKEN_COMMA: ',';
TOKEN_SEMICOLON: ';';
TOKEN_COLON: ':';
TOKEN_DOT: '.';

/** Arithmetic Operators **/
TOKEN_PLUS: '+';
TOKEN_MINUS: '-';
TOKEN_MULT: '*';
TOKEN_DIV: '/';

TOKEN_NOT: '!';

/** Comparison Operators **/
TOKEN_NOT_EQUALS: '!=';
TOKEN_EQUALS: '=';
TOKEN_GREATER_EQUAL_THAN: '>=';
TOKEN_GREATER_THAN: '>';
TOKEN_LESS_EQUAL_THAN: '<=';
TOKEN_LESS_THAN: '<';

TOKEN_LEFT_PAREN: '(';
TOKEN_RIGHT_PAREN: ')';
TOKEN_WS: [ \t\r\n]+ -> skip;

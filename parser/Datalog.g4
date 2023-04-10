grammar Datalog;

@header{
}

@parser::members {

class AtomArg():
    def __init__(self, arg_object, arg_type, key_attribute=False):
        self.object = arg_object
        self.type = arg_type
        self.key_attribute = key_attribute

    def __str__(self):
	    return f"{self.object}, {self.type}, {self.key_attribute}"
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
      t1 = attribute {schema['attributes'].append(self.AtomArg($t1.r['name'], $t1.r['type'], $t1.r['is_key']))}
      (TOKEN_COMMA 
	   t2 = attribute {schema['attributes'].append(self.AtomArg($t2.r['name'], $t2.r['type'], $t2.r['is_key']))})*
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
    : {rule_map = {}}
		(TOKEN_NON_DEDUP {rule_map['non-dedup'] = True})? 
		(TOKEN_NON_SET_DIFF {rule_map['non-set-diff'] = True})?
                (TOKEN_DEDUP_ONLY {rule_map['dedup-only'] = True})?
	    h = head {rule_map['head'] = $h.r}
	    TOKEN_BODY_HEAD_SEP {rule_map['body'] = None}
	    (b = body {rule_map['body'] = $b.r})?
	    TOKEN_DOT
	  {$r = rule_map}
    ;

head returns [r]
	: a = atom
	  {$r = $a.r}
	;

body returns [r] 
    : {body_map = {'atoms':[], 'compares': [], 'assigns':[], 'negations':[]}}
	  ((b1 = atom 			{body_map['atoms'].append($b1.r)} | 
	   	b2 = compare_expr 	{body_map['compares'].append($b2.r)} | 
	  	b3 = assign 		{body_map['assigns'].append($b3.r)} | 
	    b4 = negation 		{body_map['negations'].append($b4.r)}) 
		TOKEN_COMMA)*
	  ((b5 = atom 			{body_map['atoms'].append($b5.r)} |
		b6 = compare_expr 	{body_map['compares'].append($b6.r)} |
		b7 = assign		    {body_map['assigns'].append($b7.r)} |
		b8 = negation 		{body_map['negations'].append($b8.r)}))
	  {$r = body_map}
	;


negation returns [r]
	: TOKEN_NOT a = atom
	  {$r = $a.r}
	;

atom returns [r]
	: {atom_map = {'name': None, 'arg_list':[]}}
	  a1 = TOKEN_ID 		  {atom_map['name'] = $a1.text}  
	  TOKEN_LEFT_PAREN 
	  (a2 = TOKEN_ID 		  {atom_map['arg_list'].append(self.AtomArg($a2.text, 'variable'))} |
	   a3 = aggregation_expr  {atom_map['arg_list'].append(self.AtomArg($a3.r, 'aggregation'))} |
	   a4 = TOKEN_ANY 		  {atom_map['arg_list'].append(self.AtomArg($a4.text, 'any'))} |
	   a5 = constant 		  {atom_map['arg_list'].append(self.AtomArg($a5.r, 'constant'))} |
	   a6 = math_expr         {atom_map['arg_list'].append(self.AtomArg($a6.r, 'math_expr'))})
	  (TOKEN_COMMA 
	   (a7 = TOKEN_ID 		  {atom_map['arg_list'].append(self.AtomArg($a7.text, 'variable'))} |
		a8 = aggregation_expr {atom_map['arg_list'].append(self.AtomArg($a8.r, 'aggregation'))}|
		a9 = TOKEN_ANY		  {atom_map['arg_list'].append(self.AtomArg($a9.text, 'any'))} |
		a10 = constant 		  {atom_map['arg_list'].append(self.AtomArg($a10.r, 'constant'))} |
		a11 = math_expr       {atom_map['arg_list'].append(self.AtomArg($a11.r, 'math_expr'))}))*
	  TOKEN_RIGHT_PAREN
	  {$r = atom_map}
	;

assign returns [r] 
	: {assign_map = {}}
	  a1 = TOKEN_ID 	  {assign_map['lhs'] = $a1.text} 
	  TOKEN_ASSIGN
	  a2 = math_expr 	  {assign_map['rhs'] = $a2.r}
	  {$r = assign_map}
	;

compare_expr returns [r]
	: {compare_map = {}}
      (c1 = TOKEN_ID      {compare_map['lhs'] = {"type": "variable", "value": $c1.text}} |
       c2 = number        {compare_map['lhs'] = {"type": "number", "value": $c2.text}})
       op = compare_op	  {compare_map['op'] = $op.r}
      (c4 = TOKEN_ID      {compare_map['rhs'] = {"type": "variable", "value": $c4.text}} |
       c5 = number        {compare_map['rhs'] = {"type": "number", "value": $c5.text}})
	  {$r = compare_map}
	; 


aggregation_expr returns [r]
	: {agg_map = {'agg_op': None, 'agg_arg': None}}
	  a1 = aggregation_op {agg_map['agg_op'] = $a1.r}
	  TOKEN_LEFT_PAREN
	  (a2 = TOKEN_ID 	  {agg_map['agg_arg'] = {'type': 'attribute', 'content': $a2.text}} |
	   a3 = math_expr     {agg_map['agg_arg'] = {'type': 'math_expr', 'content': $a3.r}})
	  TOKEN_RIGHT_PAREN 
	  {$r = agg_map}
	;

math_expr returns [r]
	: {math_map = {}}
	  (m1 = TOKEN_ID	  {math_map['lhs'] = {"type": "variable", "value": $m1.text}} |
	   m2 = number        {math_map['lhs'] = {"type": "number", "value": $m2.text}})
	   m3 = math_op		  {math_map['op'] = $m3.r}
      (m4 = TOKEN_ID 	  {math_map['rhs'] = {"type": "variable", "value": $m4.text}} |
	   m5 = number        {math_map['rhs'] = {"type": "number", "value": $m5.text}})
	  {$r = math_map}
	;

attribute returns [r]
	: a1 = non_key_attribute {$r = $a1.r}
	| a2 = key_attribute {$r = $a2.r}
	;

key_attribute returns [r]
	: {attribute_map = {'name': None, 'type': None, 'is_key': True}}
	  TOKEN_LEFT_BRACKET 
	  a1 = TOKEN_ID {attribute_map['name'] = $a1.text}
	  TOKEN_RIGHT_BRACKET
	  d1 = data_type {attribute_map['type'] = $d1.r}
	  {$r = attribute_map}
	;

non_key_attribute returns [r]
	: {attribute_map = {'name': None, 'type': None, 'is_key': False}}
	  a1 = TOKEN_ID {attribute_map['name'] = $a1.text} 
	  d1 = data_type {attribute_map['type'] = $d1.r}
	  {$r = attribute_map}
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
    | op5 = TOKEN_COUNT_DISTINCT {$r = $op5.text}
	;

math_op returns [r] 
	: op1 = TOKEN_PLUS  {$r = $op1.text}
	| op2 = TOKEN_MINUS {$r = $op2.text}
	| op3 = TOKEN_MULT  {$r = $op3.text}
	| op4 = TOKEN_DIV	{$r = $op4.text}
	;	

constant returns [r]
	: c1 = number {$r = {"type": "number", "value": $c1.r}}
	| c2 = TOKEN_STRING {$r = {"type": "string", "value": $c2.text}}
	;

number returns [r]
	: n1 = TOKEN_INTEGER {$r = {"type": "int", "value": $n1.text}}
	;

data_type returns [r]
    : dt1 = TOKEN_INT {$r = $dt1.text}
    | dt2 = TOKEN_LONG {$r = $dt2.text}	
    | dt3 = TOKEN_LONG_NULL {$r = $dt3.text}
    | dt4 = TOKEN_FLOAT {$r = $dt4.text}
    | dt5 = TOKEN_DOUBLE {$r = $dt5.text}
    | dt6 = TOKEN_VARCHAR {$r = $dt6.text}
    | dt7 = TOKEN_CHAR {$r = $dt7.text}
    | dt8 = TOKEN_DATE {$r = $dt8.text}
    | dt9 = TOKEN_DATETIME {$r = $dt9.text}
    ;

/** Declaration **/
TOKEN_EDB: 'EDB_DECL';
TOKEN_IDB: 'IDB_DECL';
TOKEN_RULE: 'RULE_DECL';

/** Constants **/
TOKEN_INTEGER: [0-9]+;
TOKEN_STRING: '\''([A-Za-z] | [0-9])+'\'';

/** Data Types **/
TOKEN_INT: ('i'|'I')('n'|'N')('t'|'T');
TOKEN_LONG: ('l'|'L')('o'|'O')('n'|'N')('g'|'G');
TOKEN_LONG_NULL: ('l'|'L')('o'|'O')('n'|'N')('g'|'G')(' ')('n'|'N')('u'|'U')('l'|'L')('l'|'L');
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
TOKEN_COUNT_DISTINCT: 'COUNT_DISTINCT';

/**  Datalog Rules **/
TOKEN_ID: [A-Za-z]([A-Za-z]|[0-9]|'_')*;
TOKEN_BODY_HEAD_SEP: ':-';
TOKEN_ANY: '_';
TOKEN_COMMA: ',';
TOKEN_SEMICOLON: ';';
TOKEN_COLON: ':';
TOKEN_DOT: '.';

/** Arithmetic Operators **/
TOKEN_ASSIGN: '=';
TOKEN_PLUS: '+';
TOKEN_MINUS: '-';
TOKEN_MULT: '*';
TOKEN_DIV: '/';
TOKEN_NOT: '!';

/** Rule Computation Flags **/
TOKEN_NON_DEDUP: '[!dedup]';
TOKEN_NON_SET_DIFF: '[!set-diff]';
TOKEN_DEDUP_ONLY: '[dedup-only]';

/** Comparison Operators **/
TOKEN_NOT_EQUALS: '!=';
TOKEN_EQUALS: '==';
TOKEN_GREATER_EQUAL_THAN: '>=';
TOKEN_GREATER_THAN: '>';
TOKEN_LESS_EQUAL_THAN: '<=';
TOKEN_LESS_THAN: '<';

TOKEN_LEFT_PAREN: '(';
TOKEN_RIGHT_PAREN: ')';
TOKEN_LEFT_BRACKET: '[';
TOKEN_RIGHT_BRACKET: ']';
TOKEN_WS: [ \t\r\n]+ -> skip;
LINE_COMMENT: '#' ~[\r\n]* -> skip;

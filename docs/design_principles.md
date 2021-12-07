# Design Principles

## If bugs can be found easily, we just comment and do no validation
For example, `Selector#From` only takes pointer of structure as input. 
Someone may think we should check if the input is pointer, but actually I think we don't need to do this 
because users can find the bug by testing or code review easily. 
So I just comment this rule.
#=
Catching the value that caused an error

A debugging trick is to catch the value that caused an error. This can be done by using a Ref{Any} as a global variable. 
The Ref{Any} is initialized with nothing and then set to the value of the argument that caused the error. 
The Ref{Any} is then inspected after the error is thrown, using the Julia workspace view.

=#
const _args = Ref{Any}()
function foo(arg1, args...)
    _args[] = deepcopy((arg1, args...))
    # implementation
end
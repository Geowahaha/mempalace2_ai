Option Explicit

If WScript.Arguments.Count < 1 Then
  WScript.Quit 1
End If

Dim shell, exe, i, arg, cmd
Set shell = CreateObject("WScript.Shell")
exe = WScript.Arguments(0)
cmd = """" & exe & """"

For i = 1 To WScript.Arguments.Count - 1
  arg = WScript.Arguments(i)
  arg = Replace(arg, """", """""")
  cmd = cmd & " """ & arg & """"
Next

shell.Run cmd, 0, False
WScript.Quit 0

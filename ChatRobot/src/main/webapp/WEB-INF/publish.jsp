<%--
  Created by IntelliJ IDEA.
  User: y
  Date: 2018/6/5
  Time: 10:47
  To change this template use File | Settings | File Templates.
--%>
<%@ page contentType="text/html;charset=UTF-8" language="java" %>
<html>
<head>
    <title>Publish Page</title>
</head>
<body>
<h2>Please enter the topic and the message you want to publish </h2>
<form method="POST" action="/SpringWeb1/redirect">
    <table>
        <tr>
            <td>topic</td>
            <td>
                <input type="text" name="topic">
            </td>
        </tr>
        <tr>
            <td>message</td>
            <td>
                <input type="text" name="message">
            </td>
        </tr>

        <tr>
            <td><input type="submit" value="Publish" /></td>
        </tr>
    </table>
</form>
</body>
</html>

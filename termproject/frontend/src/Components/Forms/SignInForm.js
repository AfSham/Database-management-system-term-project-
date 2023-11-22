import React, { useState } from "react";
import {
    Button,
    ConfigProvider,
    theme,
    Form,
    Input,
    Card,
    Typography
} from 'antd';
import {UserOutlined, LockOutlined} from "@ant-design/icons";
import "./Form.css";
import { customFetch } from "../../utils/fetchUtils";
const {Title} = Typography;


const SignInForm = () => {
    const [email, setEmail] = useState('');
    const [password, setPassword] = useState('');
    const [user, setUser] = useState('');


    const  handleSubmit = async () => {
        // const user = { email, password };
        // send the email and password to the server
        const response = await fetch(`http://localhost:3300/api/users?email=${email}&password=${password}`).then(res => res.json());
        console.log("GOT RESPONSE: ", response)
        // set the state of the user
        if(response.length < 1){
            alert("Invalid email or password")
            return;
        }
        // store the user in localStorage
        console.log("SETTING USER: ", response[0])
        localStorage.setItem('user', JSON.stringify(response[0]));
        setUser(response[0]);
        window.location.href = "http://localhost:3000/workspace";
      };


    return (
        <ConfigProvider theme={
            {algorithm: theme.defaultAlgorithm}
        }>
            <div class="body">
                <div style={
                    {
                        display: "flex",
                        justifyContent: "center",
                        alignItems: "center",
                        height: "100vh"
                    }
                }>
                    <Card style={
                        {width: 500}
                    }>
                        <div style={
                            {
                                display: "flex",
                                justifyContent: "center"
                            }
                        }>
                            <Title level={2}>Log In
                            </Title>
                        </div>
                        <Form name="normal_login" className="login-form"
                            initialValues={
                                {remember: true}
                        }>
                            <Form.Item name="email"
                                rules={
                                    [{
                                            required: true,
                                            message: "Please input your email!"
                                        }]
                            }>
                                <Input prefix={
                                        <UserOutlined
                                    className="site-form-item-icon"/>
                                    }
                                    value = { email }
                                    id="email"
                                    onChange=
                                    {(e) => setEmail(e.target.value)}
                                    placeholder="Email"/>
                            </Form.Item>
                            <Form.Item name="password"
                                rules={
                                    [{
                                            required: true,
                                            message: "Please input your Password!"
                                        }]
                            }>
                                <Input prefix={
                                        <LockOutlined
                                    className="site-form-item-icon"/>
                                    }
                                    type="password"
                                    id="password"
                                    value = { password }
                                    onChange=
                                    {(e) => setPassword(e.target.value)}
                                    placeholder="Password"/>
                            </Form.Item>
    
                            <Form.Item>
                                <Button type="primary" htmlType="submit" className="login-form-button" onClick={
                                        () => handleSubmit()
                                    } block>
                                    Log in
                                </Button>
                                Don't have an account?{" "}
                                <a href="http://localhost:3000/signup">
                                    Sign up here
                                </a>
                            </Form.Item>
                        </Form>
                    </Card>
                </div>
            </div>
        </ConfigProvider>
    );
};

export default SignInForm;
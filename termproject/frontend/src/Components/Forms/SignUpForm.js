import React, {useState} from "react";
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
import {customFetch} from '../../utils/fetchUtils';
const {Title} = Typography;

const SignUpForm = () => {

    const [name, setName] = useState(null);
    const [email, setEmail] = useState(null);
    const [password, setPassword] = useState(null);

    const handleInputChange = (e) => {
        const {id, value} = e.target;
        if (id === "name") {
            setName(value);
        }
        if (id === "email") {
            setEmail(value);
        }
        if (id === "password") {
            setPassword(value);
        }

    }

    const handleSubmit = () => {
        addUser(name, email, password);
        window.location.href = "http://localhost:3000/signin";
    }

    const addUser = (name, email, password) => {
        console.log(name, email, password);
        // Add Lane to DB
        customFetch("http://localhost:3300/api/users", {
            name: name,
            email: email,
            password: password
        }, "POST").then((data) => {
            console.log("Added User:", data);
        });
    }

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
                            <Title level={2}>Sign Up
                            </Title>
                        </div>
                        <Form name="normal_login" className="login-form"
                            initialValues={
                                {remember: true}
                        }>
                            <Form.Item name="Name"
                                rules={
                                    [{
                                            required: true,
                                            message: "Please input your name!"
                                        }]
                            }>
                                <Input prefix={
                                        <UserOutlined
                                    className="site-form-item-icon"/>
                                    }
                                    id="name"
                                    onChange=
                                    {(e) => handleInputChange(e)}
                                    value={name}
                                    placeholder="Name"/>
                            </Form.Item>
                            <Form.Item name="email"
                                rules={
                                    [{
                                            required: true,
                                            message: "Please input your email"
                                        }]
                            }>
                                <Input prefix={
                                        <UserOutlined
                                    className="site-form-item-icon"/>
                                    }
                                    id="email"
                                    onChange=
                                    {(e) => handleInputChange(e)}
                                    value={email}
                                    placeholder="Email Address"/>
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
                                    onChange=
                                    {(e) => handleInputChange(e)}
                                    value={password}
                                    placeholder="Password"/>
                            </Form.Item>
                            <Form.Item>
                                <Button type="primary" htmlType="submit" className="login-form-button"
                                    onClick={
                                        () => handleSubmit()
                                    }
                                    block>
                                    Sign Up
                                </Button>
                                Already have an account?{" "}
                                <a href="http://localhost:3000/signin">
                                    Sign in here
                                </a>
                            </Form.Item>
                        </Form>
                    </Card>
                </div>
            </div>
        </ConfigProvider>
    );
};

export default SignUpForm;
import React, {useState} from "react";
import {Link} from "react-router-dom";
import {
    Layout,
    Menu,
    Row,
    Button,
    ConfigProvider,
    theme
} from 'antd';
import {HomeOutlined, UserOutlined} from "@ant-design/icons";
import "./NavBar.css"
import { useUser } from "../../Pages/SignInPage";


const NavBar = () => {

    // const [user, setUser] = useState();
    const user = useUser();
    const [visible, setVisible] = useState(false);
    const {Header} = Layout;
    const showDrawer = () => {
        setVisible(true);
    };

    const onClose = () => {
        setVisible(false);
    };

    // if there's a user logged in show the navbar with logout option
    if (user) {
        return(
            <Header style={
                {color: 'white'}
            }>
                <Row justify="space-between" align="middle">
                    <ConfigProvider theme={
                        {algorithm: theme.lightAlgorithm}
                    }>
                        <div className="logo"
                            style={
                                {
                                    color: "white",
                                    paddingLeft: "20px"
                                }
                        }>
                            <Link to='http://localhost:3000/'>
                                <h3 className="brand-font">-n.</h3>
                            </Link>
                        </div>
                        <div className="menu" align="middle" justify-content="right">
                            <Menu theme="dark" mode="horizontal"
                                defaultSelectedKeys={
                                    ["1"]
                            }>
                                <Menu.Item key="1"
                                    icon={<HomeOutlined/>}>
                                    <Link to='http://localhost:3000/'>
                                        Home
                                    </Link>
                                </Menu.Item>
                                <Menu.Item key="2"
                                    icon={<UserOutlined/>}>
                                    <Link to='http://localhost:3000/workspace'>
                                        My Workspace
                                    </Link>
                                </Menu.Item>
                                <Menu.Item key="3">
                                    <Button type="primary"
                                        onClick={() => {
                                            localStorage.removeItem('user');
                                            window.location.href = "http://localhost:3000/signin";
                                        }}
                                        style={
                                            {marginRight: "10px"}
                                    }>
                                            Sign Out
                                    </Button>
                                </Menu.Item>
                            </Menu>
                        </div>
                    </ConfigProvider>
                </Row>
            </Header>
        );
    };


    return (
        <Header style={
            {color: 'white'}
        }>
            <Row justify="space-between" align="middle">
                <ConfigProvider theme={
                    {algorithm: theme.lightAlgorithm}
                }>
                    <div className="logo"
                        style={
                            {
                                color: "white",
                                paddingLeft: "20px"
                            }
                    }>
                        <Link to='http://localhost:3000/'>
                            <h3 className="brand-font">-n.</h3>
                        </Link>
                    </div>
                    <div className="menu" align="middle" justify-content="right">
                        <Menu theme="dark" mode="horizontal"
                            defaultSelectedKeys={
                                ["1"]
                        }>
                            <Menu.Item key="1"
                                icon={<HomeOutlined/>}>
                                <Link to='http://localhost:3000/'>
                                    Home
                                </Link>
                            </Menu.Item>
                            {/* <Menu.Item key="2"
                                icon={<UserOutlined/>}>
                                <Link to='http://localhost:3000/workspace'>
                                    My Workspace
                                </Link>
                            </Menu.Item> */}
                            <Menu.Item key="3">
                                <Button type="primary"
                                    style={
                                        {marginRight: "10px"}
                                }>
                                    <Link to='http://localhost:3000/signin'>
                                        Sign in
                                    </Link>
                                </Button>
                                <Button>
                                    <Link to='http://localhost:3000/signup'>
                                        Sign up
                                    </Link>
                                </Button>

                            </Menu.Item>
                        </Menu>
                    </div>
                </ConfigProvider>
            </Row>
        </Header>
    );
};

export default NavBar;

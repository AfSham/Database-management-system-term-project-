import React from "react";
import { ConfigProvider, theme } from "antd";
import NavBar from "../Components/NavBar/NavBar";
import "./HomePage.css";
import { Typography, Button } from "antd";
import { Link } from "react-router-dom";
import PageLayout from "../Components/PageLayout/PageLayout";

const { Title } = Typography;

export default function HomePage() {
  return (
    <PageLayout>
      <ConfigProvider theme={{ algorithm: theme.darkAlgorithm }}>
        <div className="Welcome">
          <Title>Clonello Project Management</Title>
          <Title level={5} id="welcome_statement">
            Clonello brings all your tasks, teammates, and tools together...and
            is definitely not similar to Trello!
          </Title>
          <div className="buttonholder">
            <br></br>
            <Button size="large" type="primary">
              <Link to="http://localhost:3000/signin">Get Started</Link>
            </Button>
          </div>
        </div>
      </ConfigProvider>
    </PageLayout>
  );
}

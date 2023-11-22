import React from "react";
import { Result, Button, ConfigProvider} from "antd";

export default function ErrorPage() {
  return (
      <Result
        status="404"
        title="404"
        subTitle="Sorry, the page you visited does not exist."
      />
  );
}

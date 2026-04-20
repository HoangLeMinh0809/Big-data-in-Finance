function PageContainer({ title, subtitle, children }) {
  return (
    <div className="page-container">
      <div>
        <h1 className="page-title">{title}</h1>
        {subtitle ? <p className="page-subtitle">{subtitle}</p> : null}
      </div>
      {children}
    </div>
  );
}

export default PageContainer;
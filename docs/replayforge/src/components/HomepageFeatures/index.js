import clsx from 'clsx';
import Heading from '@theme/Heading';
import styles from './styles.module.css';

const FeatureList = [
  {
    title: 'Easy to Use',
    Svg: require('@site/static/img/undraw_docusaurus_mountain.svg').default,
    description: (
      <>
        Built for resilience from day one, ReplayForge embraces graceful data loss
        and rapid recovery, ensuring your data flows keep running even through
        restarts and errors.
      </>
    ),
  },
  {
    title: 'Access Off-Cloud Data',
    Svg: require('@site/static/img/undraw_docusaurus_tree.svg').default,
    description: (
      <>
        ReplayForge enables secure access to asynchronous data sources outside your cloud
        infrastructure, allowing you to seamlessly integrate with remote or on-premise systems.
      </>
    ),
  },
  {
    title: 'Tailscale networking',
    Svg: require('@site/static/img/undraw_docusaurus_react.svg').default,
    description: (
      <>
        Built as simple Golang binaries with Tailscale support for secure networking,
        allowing you to host relay servers on machines outside the cloud while maintaining
        secure encrypted communication between components.
      </>
    ),
  },
];

function Feature({Svg, title, description}) {
  return (
    <div className={clsx('col col--4')}>
      <div className="text--center">
        <Svg className={styles.featureSvg} role="img" />
      </div>
      <div className="text--center padding-horiz--md">
        <Heading as="h3">{title}</Heading>
        <p>{description}</p>
      </div>
    </div>
  );
}

export default function HomepageFeatures() {
  return (
    <section className={styles.features}>
      <div className="container">
        <div className="row">
          {FeatureList.map((props, idx) => (
            <Feature key={idx} {...props} />
          ))}
        </div>
      </div>
    </section>
  );
}
